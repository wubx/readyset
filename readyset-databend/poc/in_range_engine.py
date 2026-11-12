#!/usr/bin/env python3
"""Offline native-engine regression for IN/ranges, physical fences and result overflow.

Run on the validation host AFTER building databend-m2. Synthetic local rows only;
no Databend or other upstream connection. Never copies or reopens earlier runtime state.
"""
import argparse
import copy
import json
from pathlib import Path
import tempfile

from m2 import Engine, checksum


def run(binary, root):
    root.mkdir(exist_ok=False)
    checks = []

    def check(label, actual, expected):
        checks.append({'label': label, 'passed': actual == expected})
        if actual != expected:
            raise AssertionError(f'{label}: {actual!r} != {expected!r}')

    rows = [{'id': i, 'payload': 'duplicate', 'score': 7, 'note': None} for i in range(1, 10001)]
    initial = root / 'initial.jsonl'
    initial.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    declarations = root / 'declarations.json'
    declarations.write_text(json.dumps({'database': 'poc_in_range', 'table': 'source_rows', 'queries': []}))
    engine = Engine(Path(binary).resolve(), initial, root / 'engine', 'rs_in_range_budget', root / 'journal.jsonl', declarations)
    sequence = 0
    error = None
    try:
        def ddl(sql):
            result = engine.request({'op': 'cache_ddl', 'sql': sql, 'database': 'poc_in_range'})
            check('DDL: ' + sql[:100], result.get('ok'), True)
            return result

        def query(predicate, projection='payload'):
            return engine.request({'op': 'query', 'sql': f'SELECT {projection} FROM poc_in_range.source_rows WHERE {predicate}', 'database': 'poc_in_range'})

        def apply(changes):
            nonlocal sequence
            sequence += 1
            batch = {'generation': 'rs_in_range_budget', 'sequence': sequence, 'row_count': len(changes),
                     'changes_sha256': checksum(changes), 'changes': changes}
            result = engine.request({'op': 'apply', 'batch': batch})
            check('apply checkpoint ' + str(sequence), result.get('checkpoint'), sequence)
            result = engine.request({'op': 'barrier', 'rows': rows, 'missing': []})
            check('physical reader barrier ' + str(sequence), result.get('ok'), True)

        def change(action, row):
            return {'action': action, 'row_id': 'ir_' + str(row['id']), 'is_update': False, 'row': row}

        ddl('CREATE CACHE budget FROM SELECT payload FROM poc_in_range.source_rows WHERE score>=?')
        result = query('score>=7')
        check('exactly 10000 rows are local', result.get('matched'), True)
        check('exact limit preserves source duplicates', len(result['data']), 10000)
        row = {'id': 10001, 'payload': 'extra', 'score': 7, 'note': None}
        rows.append(row)
        apply([change('INSERT', row)])
        result = query('score>=7')
        check('10001 rows is a conservative miss, never truncated', result.get('matched'), False)
        check('overflow returns no partial data', 'data' in result, False)
        result = query('score>=8')
        check('reader still accepts empty range after row overflow', (result.get('matched'), result.get('data')), (True, []))
        # Escaped JSON is >4 MiB while the raw string is smaller. Validate serialized bytes.
        wide = {'id': 10002, 'payload': '\\' * (2200 * 1024), 'score': 8, 'note': None}
        rows.append(wide)
        apply([change('INSERT', wide)])
        result = query('score>=8')
        check('encoded byte overflow is a conservative miss', result.get('matched'), False)
        check('byte overflow returns no partial data', 'data' in result, False)
        old = copy.deepcopy(wide)
        wide['payload'] = 'small-again'
        apply([change('DELETE', old), change('INSERT', wide)])
        result = query('score>=8')
        check('index and route recover after bounded content update', (result.get('matched'), result.get('data')), (True, [['small-again']]))
        rows.remove(row)
        apply([change('DELETE', row)])
        # Free the same physical shape before declaring IN. Its NULL projection must be preserved.
        ddl('DROP CACHE budget')
        ddl('CREATE CACHE ids FROM SELECT id,payload,score,note FROM poc_in_range.source_rows WHERE id IN (?,?)')
        result = query('id IN (10002,10002)', 'id,payload,score,note')
        check('repeated IN key is fetched once', result.get('data'), [['10002', 'small-again', '8', None]])
        result = query('id IN (10002)', 'id,payload,score,note')
        check('another IN arity cannot reuse route by shape alone', result.get('matched'), False)
        # A new declaration of another layout is refused before migration: DROP aliases stay safe.
        result = engine.request({'op': 'cache_ddl', 'database': 'poc_in_range',
                                 'sql': 'CREATE CACHE alias FROM SELECT id,payload,score,note FROM poc_in_range.source_rows WHERE id IN (?,?,?)'})
        check('physical-shape alias rejected', result.get('rejected'), True)
        ddl('DROP CACHE ids')
        ddl('CREATE CACHE closed FROM SELECT id FROM poc_in_range.source_rows WHERE score BETWEEN ? AND ?')
        check('inclusive single-value interval', len(query('score BETWEEN 7 AND 7', 'id')['data']), 10000)
        check('inverted interval is local empty', query('score BETWEEN 8 AND 7', 'id').get('data'), [])
        check('explicit closed pair shares native BETWEEN identity', len(query('score>=7 AND score<=7', 'id')['data']), 10000)
        ddl('DROP CACHE closed')
        engine.finish()
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        engine.close()
        report = {'passed': error is None, 'error': error, 'checks': checks, 'raw_directory': str(root),
                  'scope': 'Synthetic offline engine only; not crash recovery or a Databend live run'}
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        print('Report: ' + str(root / 'report.json'), flush=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', default=None)
    args = parser.parse_args()
    root = Path(args.output) if args.output else Path(tempfile.mkdtemp(prefix='readyset-in-range-engine-')) / 'run'
    return run(args.binary, root)


if __name__ == '__main__':
    raise SystemExit(main())
