#!/usr/bin/env python3
"""Check GUI-shaped configuration through a real, isolated fork App Server.

No model turns, external service messages, or personal Codex state are used.
"""

import argparse
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--codex', required=True, type=Path)
    args = parser.parse_args()
    binary = args.codex.resolve()
    guardian = {
        'enabled': True,
        'transcript_mode': 'line',
        'async_classifier_mode': 'conversation',
        'async_classifier_conversation_token_limit': 100_000,
        'review_threshold': 0.65,
        'review_scope': {'sandboxed_exec_commands': True},
    }
    with tempfile.TemporaryDirectory(prefix='codex-desktop-compat-') as directory:
        root = Path(directory)
        home = root / 'home'
        home.mkdir(mode=0o700)
        workspace = root / 'workspace'
        workspace.mkdir()
        env = {**os.environ, 'CODEX_HOME': str(home),
               'OPENAI_API_KEY': 'sk-test-only-no-provider-calls'}
        responses = queue.Queue()
        with (root / 'server.log').open('w') as log:
            server = subprocess.Popen(
                [str(binary), 'app-server', '--stdio'], cwd=workspace, env=env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True,
            )

            def read():
                for line in server.stdout:
                    try:
                        responses.put(json.loads(line))
                    except json.JSONDecodeError:
                        pass
                responses.put(None)

            threading.Thread(target=read, daemon=True).start()

            def request(identifier, method, params):
                server.stdin.write(json.dumps({'id': identifier, 'method': method, 'params': params}) + '\n')
                server.stdin.flush()
                while True:
                    result = responses.get(timeout=45)
                    if result is None:
                        raise RuntimeError('App Server exited before replying')
                    if result.get('id') != identifier:
                        continue
                    if 'error' in result:
                        raise RuntimeError(method + ': ' + json.dumps(result['error']))
                    return result['result']

            try:
                request(1, 'initialize', {
                    'clientInfo': {'name': 'fork-desktop-compat', 'version': '1'},
                    'capabilities': {'experimentalApi': True},
                })
                server.stdin.write(json.dumps({'method': 'initialized', 'params': {}}) + '\n')
                server.stdin.flush()
                config = {'features.guardianv2': guardian}
                result = request(2, 'thread/start', {
                    'cwd': str(workspace), 'approvalPolicy': 'untrusted',
                    'sandbox': 'read-only', 'config': config,
                })
                if not result.get('thread', {}).get('id'):
                    raise RuntimeError('thread/start did not create the test thread')
                print(json.dumps({'ok': True, 'structuredGuardian': True,
                                  'threadCreated': True, 'modelTurnsStarted': 0}))
            finally:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


if __name__ == '__main__':
    main()
