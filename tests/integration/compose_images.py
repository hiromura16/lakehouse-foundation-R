"""隔離受入で固定イメージの取得とコンテナ起動を分ける。"""

import subprocess
import time


def command_tail(error, redactions=()):
    """Composeの進捗末尾を、タイムアウト時のbytes形式からも安全に返す。"""
    output = error.stderr or error.stdout or ""
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    for value in redactions:
        if value:
            output = output.replace(value, "[非表示]")
    return output[-4096:].strip()


def pull_images(run, *services, timeout, redactions=()):
    """Compose内の固定版だけを事前取得し、取得失敗は一度だけ再試行する。"""
    targets = ", ".join(services)
    for attempt in (1, 2):
        print(
            f"イメージ取得 {attempt}/2 ({timeout}秒上限) {targets}",
            flush=True,
        )
        try:
            run(
                "--progress",
                "plain",
                "--parallel",
                "2",
                "pull",
                "--ignore-buildable",
                "--policy",
                "missing",
                *services,
                timeout=timeout,
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
        ) as error:
            reason = (
                f"{error.timeout}秒でタイムアウト"
                if isinstance(error, subprocess.TimeoutExpired)
                else f"終了コード {error.returncode}"
            )
            print(f"イメージ取得失敗 ({reason}) {targets}", flush=True)
            if tail := command_tail(error, redactions):
                print(tail, flush=True)
            if attempt == 2:
                raise RuntimeError(
                    f"固定イメージの事前取得が完了しませんでした。{targets}"
                ) from None
            # ハッシュが同じ取得の再試行なので、確定済みの層を再利用できる。
            time.sleep(5)
        else:
            return
