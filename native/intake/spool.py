"""完了済みの工場ファイルをコピーし、原本と受入台帳を保全する。"""

import argparse
import csv
import fcntl
import json
import os
import re
import tempfile
from hashlib import sha256
from pathlib import Path

from scripts.landing_contract import LandingContractError, source_rows
from scripts.raw_file_input import MAX_BYTES, source_bytes

ROOT = Path(__file__).resolve().parents[2]
CODECS = {"utf-8": "utf-8-sig", "cp932": "cp932", "utf-16": "utf-16"}
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,47}")


def sync_directory(directory):
    """新規ファイルとディレクトリを記録した親ディレクトリを同期する。"""
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def prepare_parent(root, relative):
    """受け渡し先の内部に通常ディレクトリだけを順に作る。"""
    current = root
    for part in relative.parts[:-1]:
        directory = current / part
        try:
            directory.mkdir(mode=0o750)
        except FileExistsError:
            if directory.is_symlink() or not directory.is_dir():
                raise ValueError(
                    "受け渡し先の階層に通常のフォルダが必要です。"
                )
        else:
            sync_directory(current)
        current = directory


def write_once(root, relative, content):
    """同じ内容の再送を受理し、別内容の既存オブジェクトは変更しない。"""
    prepare_parent(root, relative)
    target = root / relative
    try:
        prior = source_bytes(target)
    except FileNotFoundError:
        pass
    else:
        if prior != content:
            raise ValueError("同じ配送IDの内容が異なります。")
        return
    with tempfile.NamedTemporaryFile(
        dir=target.parent, prefix=".intake-", suffix=".part", delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                if source_bytes(target) != content:
                    raise ValueError(
                        "同じ配送IDに別の内容が登録されています。"
                    )
            sync_directory(target.parent)
        finally:
            temporary.unlink(missing_ok=True)


def publish(root, relative, content):
    """原本確定後に、照合可能な完了印を同じディレクトリに公開する。"""
    write_once(root, relative, content)
    marker = Path(str(relative) + ".ready")
    expected = (sha256(content).hexdigest() + "\n").encode("ascii")
    write_once(root, marker, expected)


def canonical(logical_name):
    """業務契約の1表を選ぶ。rawは任意ファイルの原本保管を選ぶ。"""
    if logical_name == "raw":
        return None
    contract = json.loads(
        (ROOT / "medallion/sources.json").read_text(encoding="utf-8")
    )
    for table in contract["sources"]:
        if table["table"] == logical_name:
            return table
    raise ValueError("登録済みの業務データセットを指定してください。")


def plan_normalized(table, content, sender, delivery, encoding):
    """確認済みの文字コードを変換し、配置用の業務パスとデータを返す。"""
    data = content.decode(CODECS[encoding]).encode("utf-8")
    if len(data) > MAX_BYTES:
        raise LandingContractError("NORMALIZED_SIZE_LIMIT")
    canonical_path = Path(table["path"])
    delivery_key = sha256(f"{sender}/{delivery}".encode()).hexdigest()[:12]
    normalized_path = canonical_path.with_name(
        f"{canonical_path.stem}__{delivery_key}_0001{canonical_path.suffix}"
    )
    source_rows(canonical_path.suffix, data, table["columns"])
    return normalized_path, data


def accept(root, sender, delivery, source, dataset, encoding):
    """原本・変換結果・台帳を記録し、同じ配送IDで安全に再試行する。"""
    if not IDENTIFIER.fullmatch(sender) or not IDENTIFIER.fullmatch(delivery):
        raise ValueError("送信元と配送IDに英数字・ハイフン・下線を使います。")
    if root.is_symlink() or not root.is_dir():
        raise ValueError("初期設定で生成した受け渡しフォルダを指定します。")
    root = root.resolve()
    if root.is_relative_to(ROOT):
        raise ValueError("チェックアウトの外に受け渡し先を指定します。")
    source = Path(source)
    if source.name.endswith((".part", ".ready")):
        raise ValueError("完了した原本を指定してください。")
    content = source_bytes(source)
    table = canonical(dataset)
    normalized = None
    outcome = "raw"
    reason = None
    if table:
        try:
            normalized = plan_normalized(
                table, content, sender, delivery, encoding
            )
            outcome = "normalized"
        except (csv.Error, ValueError) as error:
            outcome = "rejected"
            reason = (
                str(error)
                if isinstance(error, LandingContractError)
                else type(error).__name__
            )
    raw_path = Path("original") / sender / f"{delivery}.bin"
    receipt_path = Path("receipts") / sender / f"{delivery}.json"
    receipt = {
        "format_version": 1,
        "sender": sender,
        "delivery": delivery,
        "dataset": dataset,
        "original_name": source.name,
        "original_path": raw_path.as_posix(),
        "original_sha256": sha256(content).hexdigest(),
        "original_bytes": len(content),
        "outcome": outcome,
        "reason": reason,
    }
    if normalized:
        path, data = normalized
        receipt["normalized_path"] = path.as_posix()
        receipt["normalized_sha256"] = sha256(data).hexdigest()
        receipt["encoding"] = encoding
    serialized = (json.dumps(receipt, ensure_ascii=True) + "\n").encode()
    # 同じIDへの同時配送を直列化し、原本の書換え前に台帳で衝突を確認する。
    flags = os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW
    with os.fdopen(
        os.open(root / ".intake.lock", flags, 0o600), "r+b"
    ) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        write_once(root, receipt_path, serialized)
        publish(root, raw_path, content)
        if normalized:
            path, data = normalized
            publish(root, path, data)
        # 台帳の完了印を最後に公開する。途中で停止したときは同じIDで再送。
        publish(root, receipt_path, serialized)
    return receipt


def main():
    """明示した送信元・配送IDの1ファイルだけを受け渡し先に確定する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing", type=Path, required=True)
    parser.add_argument("--sender", required=True)
    parser.add_argument("--delivery", required=True)
    parser.add_argument("--dataset", default="raw")
    parser.add_argument("--encoding", choices=CODECS, default="utf-8")
    parser.add_argument("file", type=Path)
    args = parser.parse_args()
    try:
        receipt = accept(
            args.landing,
            args.sender,
            args.delivery,
            args.file,
            args.dataset,
            args.encoding,
        )
    except (OSError, ValueError) as error:
        parser.exit(1, f"受け渡しの確定に失敗（{type(error).__name__}）。\n")
    # 端末には元の行、資格情報、絶対パスを返さない。
    print(json.dumps({key: receipt[key] for key in ("outcome", "reason")}))
    raise SystemExit(2 if receipt["outcome"] == "rejected" else 0)


if __name__ == "__main__":
    main()
