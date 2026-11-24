"""実HTTPワーカーのSIGTERMで受付を閉じ、実行中の要求を完了するか調べる。"""

import json
import os
import selectors
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import URLError
from urllib.request import Request, urlopen

CHILD = """
import sys
from threading import Lock
from types import SimpleNamespace

from native import api

real_server = api.WorkerHTTPServer


def ephemeral_server(_address, request_handler, *, stopping):
    # 実装のHTTPサーバー自体を、使い捨てのローカルポートへ束縛する。
    server = real_server(("127.0.0.1", 0), request_handler, stopping=stopping)
    print(server.server_port, flush=True)
    return server


def held_job(_core, _action, _body):
    # 親が応答を解放するまで、書込処理中であることを明示する。
    print("started", flush=True)
    assert sys.stdin.read(1) == "1"
    return {"completed": True}


api.WorkerHTTPServer = ephemeral_server
api.run_job = held_job
api.core_module = lambda: SimpleNamespace(
    STATE={"ready": False, "blocked": False},
    LOCK=Lock(),
    bootstrap=lambda: None,
)
api.serve()
"""


def read_line(child):
    """子が状態を通知しない場合は固まらず、試験の失敗にする。"""
    with selectors.DefaultSelector() as monitor:
        monitor.register(child.stdout, selectors.EVENT_READ)
        assert monitor.select(timeout=10), "子プロセスからの通知がありません。"
        return child.stdout.readline().strip()


def job(origin, token):
    """許可済みの書込要求を送り、停止時も完全なJSON応答を受け取る。"""
    request = Request(
        origin + "/run",
        data=b'{"action":"verify"}',
        headers={"Authorization": f"Bearer {token}"},
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_closed(origin):
    """停止前の要求を保持したまま、新規の受付口が閉じることを調べる。"""
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        try:
            with urlopen(origin + "/health", timeout=0.5):
                time.sleep(0.05)
        except (URLError, ConnectionError):
            return
    raise AssertionError("停止後も新しいHTTP接続が受け付けられています。")


def main():
    """待機中の書込、新規受付停止、書込完了と終了コード0を照合する。"""
    env = {
        **os.environ,
        "WORKER_TOKEN": os.urandom(32).hex(),
        "READER_TOKEN": os.urandom(32).hex(),
    }
    with subprocess.Popen(
        [sys.executable, "-c", CHILD],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            origin = f"http://127.0.0.1:{int(read_line(child))}"
            with ThreadPoolExecutor(max_workers=1) as requests:
                pending = requests.submit(job, origin, env["WORKER_TOKEN"])
                assert read_line(child) == "started"
                child.terminate()
                wait_closed(origin)
                assert child.poll() is None and not pending.done()
                child.stdin.write("1")
                child.stdin.flush()
                assert pending.result(timeout=5) == {"completed": True}
            assert child.wait(timeout=5) == 0
        finally:
            if child.poll() is None:
                child.kill()
    print("SIGTERM後の受付停止、処理の完了、終了コード0を確認しました。")


if __name__ == "__main__":
    main()
