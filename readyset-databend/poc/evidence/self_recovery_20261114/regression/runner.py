"""Validated regression matrix plus persistent restart recovery (standalone authority + restart markers)."""
import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/data1/vmbox/readyset-single-binary')
os.chdir(root)
os.environ['PATH'] = str(Path.home()/'.cargo/bin')+':'+os.environ['PATH']
os.environ['LIBCLANG_PATH'] = '/tmp/readyset-m1-libclang'
os.environ['CARGO_TARGET_DIR'] = '/data1/vmbox/readyset/target'
out = Path('/data1/vmbox/readyset-retry-regression-'+time.strftime('%Y%m%d-%H%M%S'))
out.mkdir()
print('RESULTS='+str(out),flush=True)
binary = '/data1/vmbox/readyset/target/release/readyset-databend'
endpoint = 'http://192.168.191.96:8000'
report = {'output':str(out),'steps':[],'passed':False}
steps = [
 ('fmt',['cargo','fmt','--check','-p','readyset-databend','-p','dataflow-state','-p','readyset-server','-p','readyset-client']),
 ('clippy',['cargo','clippy','--locked','-p','readyset-databend','-p','dataflow-state','-p','readyset-server','-p','readyset-client','--all-targets','--no-deps','-j','8','--','-D','warnings']),
 ('rust',['cargo','test','--locked','-p','readyset-databend','-j','8','--','--test-threads=1']),
 ('sync_wal',['cargo','test','--locked','-p','dataflow-state','-j','8','--lib','sync_wal_tests']),
 ('reopen',['cargo','test','--locked','-p','dataflow-state','-j','8','--lib','replication_offset_databend_rows_and_checkpoint_reopen']),
 ('live_schema',['env','DATABEND_TEST_ENDPOINT='+endpoint,'cargo','test','--locked','-p','readyset-databend','-j','8','--lib','engine::live_schema_tests','--','--ignored','--nocapture']),
 ('release',['cargo','build','--locked','--release','-p','readyset-databend','--bin','readyset-databend','-j','8']),
 ('python',['python3','-B','-m','unittest','discover','-s','readyset-databend/poc','-p','test_*.py']),
 ('native_server_offsets',['cargo','test','--locked','-p','readyset-server','-j','8','--lib','controller::tests::replication_offsets']),
 ('native_client',['cargo','test','--locked','-p','readyset-client','-j','8','--lib']),
 ('native_dataflow',['cargo','test','--locked','-p','readyset-dataflow','-j','8','--lib']),
]
for mode in ['full','partial']:
 steps.append(('multi_'+mode,['python3','-B','readyset-databend/poc/multitable_verify.py','--endpoint',endpoint,'--user','root','--gateway-binary',binary,'--output',str(out/('multi_'+mode)),'--confirm-create']+(['--partial'] if mode=='partial' else [])))
for mode in ['full','partial','oracle']:
 steps.append(('gateway_'+mode,['python3','-B','readyset-databend/poc/gateway_verify.py','--endpoint',endpoint,'--user','root','--gateway-binary',binary,'--output',str(out/('gateway_'+mode)),'--confirm-create','--sql-declarations','--native-queries','--in-range-queries']+([] if mode=='full' else ['--'+mode])))
for mode in ['full','partial']:
 steps.append(('pk_'+mode,['python3','-B','readyset-databend/poc/pk_change_verify.py','--endpoint',endpoint,'--user','root','--gateway-binary',binary,'--output',str(out/('pk_'+mode)),'--confirm-create']+(['--partial'] if mode=='partial' else [])))
steps.append(('restart_recovery',['python3','-B','readyset-databend/poc/restart_recovery_verify.py','--endpoint',endpoint,'--gateway-binary',binary,'--output',str(out/'restart_recovery'),'--confirm-create']))
steps.append(('bootstrap_recovery',['python3','-B','readyset-databend/poc/bootstrap_recovery_verify.py','--endpoint',endpoint,'--gateway-binary',binary,'--output',str(out/'bootstrap_recovery'),'--confirm-create']))
for name,command in steps:
 print('START '+name,flush=True)
 started=time.monotonic()
 with (out/(name+'.log')).open('w') as log:
  result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
 report['steps'].append({'name':name,'command':command,'exit_code':result.returncode,'seconds':round(time.monotonic()-started,3)})
 (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
 print(('PASS ' if result.returncode==0 else 'FAIL ')+name,flush=True)
 print('\n'.join((out/(name+'.log')).read_text().splitlines()[-5:] if result.returncode==0 else (out/(name+'.log')).read_text().splitlines()[-50:]),flush=True)
 if result.returncode and name in ['fmt','clippy','rust','sync_wal','reopen','release']:break
report['passed']=len(report['steps'])==len(steps) and all(s['exit_code']==0 for s in report['steps'])
(out/'report.json').write_text(json.dumps(report,indent=2)+'\n')
print('REPORT='+str(out/'report.json'),flush=True)
raise SystemExit(0 if report['passed'] else 1)
