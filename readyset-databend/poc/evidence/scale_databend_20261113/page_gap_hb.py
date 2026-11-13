import sys, time, json, threading
sys.argv=[sys.argv[0]]
import importlib.util
spec=importlib.util.spec_from_file_location('s','/tmp/databend_scale.py'); s=importlib.util.module_from_spec(spec); spec.loader.exec_module(s)
DB='readyset_gateway_scale_20261113_144428'
db=s.Db(); hb=s.Db()
def q(sql): return db.run(sql)['sample']
for gap in [90, 300]:
    q(f'UPDATE {DB}.t SET score = score + 1 WHERE id % 10 = 0')   # 2M change rows
    db.run('BEGIN')
    n, r = db.http('/v1/query', {'sql': f'SELECT {s.COLS} FROM {DB}.s WITH CONSUME', 'session': db.session,
                                 'pagination': {'wait_time_secs': 5, 'max_rows_per_page': 100000}})
    db.session = r.get('session') or db.session
    qid, node = r['id'], r['node_id']
    stop = threading.Event(); beats = []
    def beat():
        while not stop.wait(20):
            _, resp = hb.http('/v1/session/heartbeat', {'node_to_queries': {node: [qid]}})
            beats.append(resp)
    rows = len(r.get('data') or []); err=None; slept=False
    while r.get('next_uri') and not r['next_uri'].endswith('/final'):
        if not slept and rows >= 300000:
            t = threading.Thread(target=beat, daemon=True); t.start()
            print(f'gap {gap}: read {rows} rows, pausing {gap}s with heartbeat every 20s', flush=True)
            time.sleep(gap); slept=True; stop.set(); t.join()
        n, r = db.http(r['next_uri'])
        if r.get('session'): db.session = r['session']
        if r.get('error'): err = r['error']; break
        rows += len(r.get('data') or [])
    if r.get('next_uri','').endswith('/final'): db.http(r['next_uri'])
    print(f'  heartbeats sent={len(beats)} responses={beats[:2]}', flush=True)
    print(f'  rows read={rows} state={r.get("state")} error={json.dumps(err)[:200] if err else None}', flush=True)
    if err is None and rows == 2000000:
        db.run('COMMIT'); print('  COMMIT (read complete)', flush=True)
    else:
        db.run('ROLLBACK'); print('  ROLLBACK (read incomplete)', flush=True)
    db.session.pop('txn_state', None)
    print('  pending change rows after', q(f'SELECT count(*) FROM {DB}.s'), flush=True)
    q(f'SELECT id FROM {DB}.s WITH CONSUME WHERE 1 = 0')
