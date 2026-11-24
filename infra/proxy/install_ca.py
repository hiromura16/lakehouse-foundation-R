#!/usr/bin/env python3
"""既存の信頼先を保持し、ビルドするイメージに公開CA証明書を追加する。

OSの証明書ディレクトリとバンドルを更新する。keytool が使える場合は、
ハッシュから作った別名で既存のJava信頼ストアにも取り込む。
任意入力が無いか空なら省略する。入力には、シンボリックリンクでない
1 MiB以下の通常のPEMファイルを使い、証明書は64個まで含められる。
秘密鍵を含む入力は拒否する。追加した信頼設定は生成イメージに残る。
"""

import argparse
import json
import os
import re
import shutil
import ssl
import stat
import subprocess
import tempfile
from hashlib import sha256
from pathlib import Path

CERTIFICATES = re.compile(
    rb"-----BEGIN CERTIFICATE-----\s+.+?\s+-----END CERTIFICATE-----",
    re.DOTALL,
)
BUNDLE = Path("/etc/ssl/certs/ca-certificates.crt")
CERT_DIR = Path("/usr/local/share/ca-certificates")


def certificates(content, strict=False):
    """X.509のPEMブロックを検証する。ビルド入力には証明書だけを許可する。"""
    if b"PRIVATE KEY" in content:
        raise ValueError("Private keys are never accepted as CA input.")
    matches = list(CERTIFICATES.finditer(content))
    if not matches or (strict and len(matches) > 64):
        raise ValueError("A bounded PEM certificate chain is required.")
    if strict and CERTIFICATES.sub(b"", content).strip():
        raise ValueError("CA input must contain PEM certificates only.")
    result = []
    for match in matches:
        pem = match.group() + b"\n"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cadata=pem.decode("ascii"))
        der = ssl.PEM_cert_to_DER_cert(pem.decode("ascii"))
        result.append((sha256(der).hexdigest(), pem, der))
    return result


def atomic_write(path, content, permissions=0o644):
    """書込途中のバンドルを参照されないよう、公開証明書を不可分に保存する。"""
    descriptor, temporary = tempfile.mkstemp(prefix=".ca-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, permissions)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def java_store():
    """インストール済みのJREから、置き換えずに既存の信頼ストアを探す。"""
    tool = shutil.which("keytool")
    if tool is None:
        return None
    homes = []
    if os.environ.get("JAVA_HOME"):
        homes.append(Path(os.environ["JAVA_HOME"]))
    homes.append(Path(tool).resolve().parents[1])
    for home in homes:
        for relative in ["lib/security/cacerts", "jre/lib/security/cacerts"]:
            store = home / relative
            if store.is_file():
                return tool, store
    raise ValueError("Existing Java truststore was not found.")


def install_java(entries, files):
    """ハッシュを別名に使って証明書を取り込み、既存のJREエントリを保持する。"""
    target = java_store()
    if target is None:
        return 0
    tool, store = target
    count = 0
    for digest, _, der in entries:
        alias = "lakehouse-corporate-" + digest
        arguments = [
            tool,
            "-keystore",
            str(store),
            "-storepass",
            "changeit",
            "-alias",
            alias,
        ]
        existing = subprocess.run(
            arguments + ["-exportcert"], capture_output=True, check=False
        )
        if existing.returncode == 0:
            if existing.stdout != der:
                raise ValueError("A conflicting Java CA alias exists.")
            continue
        result = subprocess.run(
            arguments
            + ["-importcert", "-noprompt", "-file", str(files[digest])],
            capture_output=True,
            check=False,
        )
        if result.returncode:
            raise ValueError("Java CA import failed; existing trust is kept.")
        count += 1
    return count


def install(ca_file, *, cert_dir=CERT_DIR, bundle=BUNDLE, java=True):
    """OSと任意のJREの信頼先を保ち、上限付きの公開証明書チェーンを導入する。

    返すのは状態と証明書数だけ。入力が無いか空の場合は処理を省く。
    導入には、選択した証明書ディレクトリとOSのバンドルへの書込権限がいる。
    Javaにも追加する場合は、既存のJava信頼ストアへの書込権限も必要になる。
    """
    source = Path(ca_file)
    if not source.exists() or source == Path("/dev/null"):
        return {"status": "skipped", "certificates": 0, "java_certificates": 0}
    if (
        not source.is_file()
        or source.is_symlink()
        or source.stat().st_size > 1024 * 1024
    ):
        raise ValueError("CA input must be a bounded regular public PEM file.")
    content = source.read_bytes()
    if not content.strip():
        return {"status": "skipped", "certificates": 0, "java_certificates": 0}
    entries = certificates(content, strict=True)
    # OSの更新ツールがバンドルを再生成する前に、既存のルート証明書を保持する。
    target = Path(bundle).resolve()
    original = target.read_bytes()
    roots = certificates(original)
    directory = Path(cert_dir)
    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    for digest, pem, _ in entries:
        path = directory / ("lakehouse-corporate-" + digest + ".crt")
        if path.is_symlink():
            raise ValueError("CA certificate paths must not use symlinks.")
        if path.exists() and path.read_bytes() != pem:
            raise ValueError("A conflicting CA certificate file exists.")
        if not path.exists():
            atomic_write(path, pem)
        files[digest] = path
    update = shutil.which("update-ca-certificates")
    if update is not None:
        result = subprocess.run([update], capture_output=True, check=False)
        if result.returncode:
            raise ValueError("OS CA update failed.")
    current = target.read_bytes()
    known = {item[0] for item in certificates(current)}
    merged = current.rstrip() + b"\n"
    for digest, pem, _ in roots + entries:
        if digest not in known:
            merged += pem
            known.add(digest)
    atomic_write(target, merged, stat.S_IMODE(target.stat().st_mode))
    imported = install_java(entries, files) if java else 0
    return {
        "status": "installed",
        "certificates": len(entries),
        "java_certificates": imported,
    }


def main():
    """イメージビルド時に実行し、証明書の名前やPEMは出さず件数を記録する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ca-file",
        type=Path,
        default=Path("/run/secrets/corporate_ca"),
        help=(
            "build時に追加する公開CA PEM。既定: /run/secrets/corporate_ca。"
            "未存在/空ならskip。OS/JREの信頼先を更新するため書込権限が必要"
        ),
    )
    args = parser.parse_args()
    try:
        result = install(args.ca_file)
    except (OSError, ValueError, ssl.SSLError, UnicodeError):
        raise SystemExit(
            "CA setup failed; check public PEM/base trust."
        ) from None
    print(json.dumps(result))


if __name__ == "__main__":
    main()
