"""Live IN/range extension; invoked only by gateway_verify --in-range-queries.

All SQL is confined to the parent verifier's newly created synthetic database.
Expected results come from Databend, never from a handwritten SQL evaluator.
"""
import json


def verify(v, info):
    db = v.database
    table = db + '.source_rows'
    baseline = v.http('GET', '/health')['response']

    def sync():
        checkpoint = v.http('GET', '/health')['response']['checkpoint']
        v.wait_ready(after=checkpoint)

    def create(name, projection, where):
        v.wait_ready()
        v.query(f'CREATE CACHE {name} FROM SELECT {projection} FROM {table} WHERE {where}', route='readyset')
        v.check('published cache: ' + name, name in v.cache_names(), True)

    def drop(name):
        v.query('DROP CACHE ' + name, route='readyset')
        v.check('removed cache: ' + name, name not in v.cache_names(), True)

    def compare(projection, predicate, route='readyset', pagination=None):
        return v.native_compare(f'SELECT {projection} FROM {table} WHERE {predicate}', route, pagination)

    records = [
        (300001, 'ir-duplicate', 900000, 'same'),
        (300002, 'ir-duplicate', 900000, 'same'),
        (300003, 'Case', 900001, None),
        (300004, 'case', 900002, 'Case'),
        (300005, 'é', 900003, 'tail '),
        (300006, 'e\u0301', 900004, 'different'),
        (300007, "O'Reilly", 900005, 'same'),
        (300008, 'back\\slash', 900006, None),
        (300009, 'ir-low-score', -9223372036854775808, None),
        (300010, 'ir-high-score', 9223372036854775807, 'high'),
        (-9223372036854775808, 'ir-low-id', -17, 'same'),
        (9223372036854775807, 'ir-high-id', -16, None),
    ]
    values = ','.join(f'({key},{v.sql_string(payload)},{score},{"NULL" if note is None else v.sql_string(note)})'
                      for key, payload, score, note in records)
    v.source.sql(f'INSERT INTO {table} VALUES {values}')
    sync()

    specs = [
        ('ir_in_score', 'payload,note', 'score IN (?,?,?)'),
        ('ir_in_pair', 'id,payload', 'note=? AND score IN (?,?)'),
        ('ir_in_cross', 'id,note', 'score IN (?,?) AND id IN (?,?,?)'),
        ('ir_in_text', 'id,payload', 'payload IN (?,?)'),
        ('ir_in_ids', 'id,payload,score,note', 'id IN (?,?)'),
        ('ir_in_note', 'id,payload', 'note IN (?,?)'),
    ]
    for spec in specs:
        create(*spec)
    v.check('IN catalog remains bounded', len(v.cache_names()), 8)
    bag = 'score IN (900000,900001,900000)'
    rows, _, first = compare('payload,note', bag, pagination={'max_rows_per_page': 1, 'max_rows_in_buffer': 1})
    v.check('IN duplicate keys do not multiply duplicate source rows', rows.count(['ir-duplicate', 'same']), 2)
    v.check('IN first page is bounded', len(first['response']['data']), 1)
    compare('payload,note', 'score IN (999990,999991,999992)')
    compare('payload,note', 'score IN (900000,999991,900000)')
    compare('id,payload', "note='same' AND score IN (900000,900005)")
    compare('id,note', 'score IN (900000,900001) AND id IN (300001,300002,300003)')
    compare('id,note', 'score IN (900000,900001,900002) AND id IN (300001,300002)', 'databend')
    compare('payload,note', 'score IN (900000,900001)', 'databend')
    compare('payload,note', 'score IN (900000,900001,900002,900003)', 'databend')
    compare('id,payload,score,note', 'id IN (-9223372036854775808,9223372036854775807)')
    for pair in [('Case', 'case'), ('é', 'e\u0301'), ("O'Reilly", 'back\\slash'), ('', 'not-there')]:
        compare('id,payload', 'payload IN (' + ','.join(map(v.sql_string, pair)) + ')')
    compare('id,payload', "note IN ('same','same')")
    compare('id,payload', "note IN ('Case','tail ')")
    compare('id,payload', "note IN ('same',NULL)", 'databend')
    compare('payload,note', "score IN ('900000','900001','900002')", 'databend')
    compare('payload,note', 'score NOT IN (900000,900001,900002)', 'databend')
    for predicate in ['score IN (?,?)', 'score IN (1,2,3)', 'score NOT IN (?,?,?)',
                      'score IN (?,NULL,?)', '(id,score) IN ((?,?),(?,?))',
                      'score IN (?,?) AND id>?']:
        v.rejected_cache_sql(f'CREATE CACHE ir_bad FROM SELECT payload,note FROM {table} WHERE {predicate}')
    too_many = 'id IN (' + ','.join(['?'] * 100) + ') AND score IN (' + ','.join(['?'] * 101) + ')'
    v.rejected_cache_sql(f'CREATE CACHE ir_bad FROM SELECT id FROM {table} WHERE {too_many}')
    # The full-row IN cache must not be mistaken for the protected full-row equality barrier.
    drop('ir_in_ids')
    v.point(7)
    create(*specs[4])
    v.source.sql(f"UPDATE {table} SET payload='ir-moved',score=900007,note='new-note' WHERE id=300001")
    v.source.sql(f'DELETE FROM {table} WHERE id=300002')
    v.source.sql(f"UPDATE {table} SET note='same' WHERE id=300003")
    v.source.sql(f'UPDATE {table} SET note=NULL WHERE id=300004')
    sync()
    compare('payload,note', bag)
    compare('id,payload', "note='same' AND score IN (900000,900001)")
    compare('id,note', 'score IN (900000,900007) AND id IN (300001,300002,300003)')
    compare('id,payload', "payload IN ('ir-duplicate','ir-moved')")
    compare('id,payload', "note IN ('same','new-note')")
    drop('ir_in_note')
    create(*specs[5])
    compare('id,payload', "note IN ('same','new-note')")

    import databend_driver
    port = v.origin.rsplit(':', 1)[1]
    conn = databend_driver.BlockingDatabendClient(f'databend://root@127.0.0.1:{port}/{db}?sslmode=disable').get_conn()
    sql = f'SELECT id,payload FROM {table} WHERE payload IN (\'Case\',\'case\')'
    expected = [tuple(row) for row in v.source.sql(sql)]
    before = v.http('GET', '/health')['response']['counters']['readyset']
    actual = [tuple(str(value) if value is not None else None for value in row.values()) for row in conn.query_all(sql)]
    v.check('native driver IN values', sorted(actual), sorted(expected))
    v.check('native driver IN is local', v.http('GET', '/health')['response']['counters']['readyset'], before + 1)
    conn.close()
    for name, _, _ in specs:
        drop(name)

    v.source.sql(f"INSERT INTO {table} VALUES (300020,'ir-duplicate',900000,'same'),(300021,'ir-duplicate',900000,'same')")
    sync()
    ranges = [
        ('ir_gt', 'id,payload,score,note', 'score>?'),
        ('ir_le', 'payload,note', 'score<=?'),
        ('ir_between', 'id,score', 'score BETWEEN ? AND ?'),
        ('ir_open', 'id,note', 'score>=? AND score<?'),
        ('ir_eq', 'id,payload', 'payload=? AND score>? AND score<=?'),
        ('ir_note', 'id,payload', 'score BETWEEN ? AND ? AND note=?'),
    ]
    for spec in ranges:
        create(*spec)
    for bound in [-9223372036854775808, -17, 0, 900000, 900007, 9223372036854775807]:
        compare('id,payload,score,note', f'score>{bound}')
        compare('payload,note', f'score<={bound}')
    for lower, upper in [(-9223372036854775808, -9223372036854775808),
                         (9223372036854775807, 9223372036854775807),
                         (-17, -16), (900000, 900002), (900007, 900007), (2, 1)]:
        compare('id,score', f'score BETWEEN {lower} AND {upper}')
        compare('id,note', f'score>={lower} AND score<{upper}')
    rows, _, _ = compare('payload,note', 'score<=900000', pagination={'max_rows_per_page': 2, 'max_rows_in_buffer': 2})
    v.check('range preserves duplicate projected source rows', rows.count(['ir-duplicate', 'same']), 2)
    for lower, upper in [(899999, 900000), (900000, 900000), (900000, 900001), (900001, 900000)]:
        compare('id,payload', f"payload='ir-duplicate' AND score>{lower} AND score<={upper}")
    compare('id,payload', "score BETWEEN 900000 AND 900007 AND note='same'")
    compare('id,payload', "score BETWEEN 900000 AND 900007 AND note=NULL", 'databend')
    for predicate in ["payload>'a'", 'score NOT BETWEEN 1 AND 2', 'score>0 AND id>0',
                      'score>=0 AND score>=1', 'score>1.0']:
        compare('id,payload', predicate, 'databend')
    v.wait_ready()
    overflow = v.http('POST', '/v1/query', {'sql': f'SELECT id,payload FROM {table} WHERE score>9223372036854775808'})
    v.check('out-of-Int64 range is forwarded, not locally coerced', overflow['route'], 'databend')
    response = overflow['response']
    for _ in range(20):
        if response.get('error') or not response.get('next_uri') or response['next_uri'].endswith('/final'):
            break
        response = v.http('GET', response['next_uri'])['response']
    v.check('Databend numeric overflow error is preserved', bool(response.get('error')), True)
    if overflow['response'].get('final_uri'):
        v.http('GET', overflow['response']['final_uri'])
    refreshed = v.wait_ready()
    v.check('source semantic error permits next fenced refresh', refreshed['failed'], None)
    for predicate in ['payload>?', 'score NOT BETWEEN ? AND ?', 'score>? AND id>?',
                      'score>=? AND score>=?', 'score BETWEEN ? AND ? AND score>?']:
        v.rejected_cache_sql(f'CREATE CACHE ir_bad FROM SELECT id,payload FROM {table} WHERE {predicate}')
    v.source.sql(f'UPDATE {table} SET score=900009 WHERE id=300020')
    v.source.sql(f'DELETE FROM {table} WHERE id=300021')
    v.source.sql(f'UPDATE {table} SET score=900002 WHERE id=300003')
    v.source.sql(f'UPDATE {table} SET score=900001 WHERE id=300004')
    v.source.sql(f"UPDATE {table} SET payload='Case',note=NULL WHERE id=300005")
    sync()
    for projection, predicate in [
        ('id,payload,score,note', 'score>900000'),
        ('payload,note', 'score<=900000'),
        ('id,score', 'score BETWEEN 900000 AND 900000'),
        ('id,note', 'score>=900001 AND score<900003'),
        ('id,payload', "payload='Case' AND score>900001 AND score<=900003"),
        ('id,payload', "payload='ir-duplicate' AND score>899999 AND score<=900000"),
        ('id,payload', "score BETWEEN 900000 AND 900007 AND note='same'"),
    ]:
        compare(projection, predicate)
    conn = databend_driver.BlockingDatabendClient(f'databend://root@127.0.0.1:{port}/{db}?sslmode=disable').get_conn()
    sql = f"SELECT id,payload FROM {table} WHERE payload='Case' AND score>900001 AND score<=900003"
    expected = [tuple(row) for row in v.source.sql(sql)]
    before = v.http('GET', '/health')['response']['counters']['readyset']
    actual = [tuple(str(value) if value is not None else None for value in row.values()) for row in conn.query_all(sql)]
    v.check('native driver mixed range values', sorted(actual), sorted(expected))
    v.check('native driver mixed range is local', v.http('GET', '/health')['response']['counters']['readyset'], before + 1)
    conn.close()
    drop('ir_note')
    create('ir_lt', 'note', 'score<?')
    for bound in [-9223372036854775808, -17, 900000, 9223372036854775807]:
        compare('note', f'score<{bound}')
    drop('ir_lt')
    create('ir_ge', 'note', 'score>=?')
    for bound in [-9223372036854775808, 900000, 9223372036854775807]:
        compare('note', f'score>={bound}')
    drop('ir_ge')
    create('ir_id_range', 'payload,note', 'id BETWEEN ? AND ?')
    compare('payload,note', 'id BETWEEN -9223372036854775808 AND -9223372036854775808')
    compare('payload,note', 'id BETWEEN 9223372036854775807 AND 9223372036854775807')
    compare('payload,note', 'id BETWEEN 300001 AND 300010')
    drop('ir_id_range')
    create('ir_id_mixed', 'payload,note', 'score=? AND id>=? AND id<?')
    compare('payload,note', 'score=900001 AND id>=300001 AND id<300010')
    compare('payload,note', 'score=-17 AND id>=-9223372036854775808 AND id<300010')
    drop('ir_id_mixed')
    create('ir_multi_eq', 'id,payload', 'note=? AND payload=? AND score>? AND score<=?')
    compare('id,payload', "note='same' AND payload='Case' AND score>900001 AND score<=900003")
    compare('id,payload', "note='not-there' AND payload='Case' AND score>900001 AND score<=900003")
    drop('ir_multi_eq')
    # Native controller view identity remains shared across equivalent BETWEEN and >=/<= forms.
    v.rejected_cache_sql(f'CREATE CACHE ir_alias FROM SELECT id,score FROM {table} WHERE score>=? AND score<=?')
    drop('ir_gt')
    v.point(7)
    compare('id,score', 'score BETWEEN 900000 AND 900009')
    create(*ranges[0])
    compare('id,payload,score,note', 'score>900000')
    for name in ['ir_gt', 'ir_le', 'ir_between', 'ir_open', 'ir_eq']:
        drop(name)
    v.source.sql(f'DELETE FROM {table} WHERE id>=300000 OR id=-9223372036854775808')
    sync()
    v.point(7)
    now = v.http('GET', '/health')['response']
    v.check('IN and range preserve engine process', now['engine_pid'], baseline['engine_pid'])
    v.check('IN and range preserve generation', now['generation'], info['generation'])
    source_log = [json.loads(line) for line in (v.output / 'service/source_queries.jsonl').read_text().splitlines()]
    v.check('IN and range share exactly one Stream', sum(r.get('sql', '').startswith('CREATE STREAM ') for r in source_log), 1)
    v.check('IN and range leave initial public caches intact', v.cache_names(), ['gateway_by_id', 'gateway_payload'])
