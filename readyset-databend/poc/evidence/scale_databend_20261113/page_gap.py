import sys, time, json
sys.argv=[sys.argv[0]]
import importlib.util
spec=importlib.util.spec_from_file_location('s','/tmp/databend_scale.py'); s=importlib.util.module_from_spec(spec); spec.loader.exec_module(s)
DB='readyset_gateway_scale_20261113_144428'
db=s.Db()
def q(sql):
    r=db.run(sql); return r['sample']
for gap in [90]:
    q(f'UPDATE {DB}.t SET score = score + 1 WHERE id % 10 = 0')   # 2M change rows
    print('gap', gap, 'has_data before', q(f"SELECT has_data FROM system.streams WHERE database='{DB}' AND name='s'"), flush=True)
    db.run('BEGIN')
    n, r = db.http('/v1/query', {'sql': f'SELECT {s.COLS} FROM {DB}.s WITH CONSUME', 'session': db.session,
                                 'pagination': {'wait_time_secs': 5, 'max_rows_per_page': 100000}})
    db.session = r.get('session') or db.session
    rows = len(r.get('data') or []); t0=time.monotonic(); err=None; slept=False
    while r.get('next_uri') and not r['next_uri'].endswith('/final'):
        if not slept and rows >= 300000:
            print(f'  read {rows} rows, sleeping {gap}s before next page', flush=True); time.sleep(gap); slept=True
        n, r = db.http(r['next_uri'])
        if r.get('session'): db.session = r['session']
        if r.get('error'): err = r['error']; break
        rows += len(r.get('data') or [])
    if r.get('next_uri','').endswith('/final'): db.http(r['next_uri'])
    print(f'  rows read={rows} error={json.dumps(err)[:300] if err else None} txn_state={db.session.get("txn_state")}', flush=True)
    try:
        db.run("ROLLBACK"); print("  ROLLBACK ok", flush=True)
    except Exception as e:
        print('  COMMIT error', str(e)[:300], flush=True)
        try: db.run('ROLLBACK')
        except Exception as e2: print('  ROLLBACK error', str(e2)[:200])
    db.session.pop('txn_state', None)
    print('  has_data after', q(f"SELECT has_data FROM system.streams WHERE database='{DB}' AND name='s'"),
          'pending change rows', q(f'SELECT count(*) FROM {DB}.s'), flush=True)
    q(f'SELECT count(*) FROM {DB}.s WITH CONSUME')   # reset for next gap
