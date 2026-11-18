#!/usr/bin/env python3
"""Round-robin HTTP reverse proxy for multi-node tests: every request (not every connection)
goes to the next backend, like an L7 load balancer without session affinity.

Usage: lb_round_robin.py --listen 127.0.0.1:8022 --backend http://127.0.0.1:8020 --backend ...
Writes per-backend request counts to --stats (JSON) once a second. Headers (including
Cookie / Set-Cookie and X-DATABEND-*) and bodies pass through unchanged; Authorization is
never logged.
"""
import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import itertools
import json
import threading
import time
import urllib.parse

HOP = {'connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'te', 'trailer', 'upgrade'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--listen', required=True)
    p.add_argument('--backend', action='append', required=True)
    p.add_argument('--stats', required=True)
    args = p.parse_args()
    backends = [urllib.parse.urlparse(b) for b in args.backend]
    order = itertools.cycle(range(len(backends)))
    lock = threading.Lock()
    stats = {'requests': [0] * len(backends), 'errors': [0] * len(backends), 'by_path_kind': {}}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *a):
            pass

        def proxy(self):
            with lock:
                i = next(order)
                stats['requests'][i] += 1
                kind = ('lifecycle' if self.path.startswith('/v1/query/') else self.path.split('?')[0])
                stats['by_path_kind'][kind] = stats['by_path_kind'].get(kind, 0) + 1
            n = int(self.headers.get('Content-Length') or 0)
            body = self.rfile.read(n) if n else None
            b = backends[i]
            try:
                conn = http.client.HTTPConnection(b.hostname, b.port, timeout=700)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP and k.lower() != 'host'}
                headers['Host'] = f'{b.hostname}:{b.port}'
                conn.request(self.command, self.path, body=body, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                self.send_response(resp.status)
                for k, v in resp.getheaders():
                    if k.lower() not in HOP and k.lower() != 'content-length':
                        self.send_header(k, v)
                self.send_header('Content-Length', str(len(data)))
                self.send_header('X-Test-Backend', str(i))
                self.end_headers()
                self.wfile.write(data)
                conn.close()
            except Exception as e:
                with lock:
                    stats['errors'][i] += 1
                msg = json.dumps({'error': f'lb backend {i}: {e}'}).encode()
                self.send_response(502)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)

        do_GET = do_POST = do_PUT = do_DELETE = proxy

    def dump():
        while True:
            with lock:
                text = json.dumps(stats)
            with open(args.stats, 'w') as f:
                f.write(text)
            time.sleep(1)

    threading.Thread(target=dump, daemon=True).start()
    host, port = args.listen.rsplit(':', 1)
    server = ThreadingHTTPServer((host, int(port)), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == '__main__':
    main()
