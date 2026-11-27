#!/usr/bin/env python3
"""固定した上流イメージの識別子とオフラインOCIアーカイブを検証する。"""

import argparse
import gzip
import hashlib
import json
import re
import subprocess
import sys
import tarfile
import zlib
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "infra" / "cdc-stage3"
STAGE3_NAMESPACE = "foundation-r-cdc-stage3"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
MAX_JSON = 4 * 1024 * 1024
MAX_ARCHIVE = 8 * 1024**3
MAX_EXPANDED_LAYER = 2 * 1024**3
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
DOCKER_INDEX = "application/vnd.docker.distribution.manifest.list.v2+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
ACCEPT = ", ".join((OCI_INDEX, DOCKER_INDEX, OCI_MANIFEST, DOCKER_MANIFEST))
SOURCES = {
    "kafka": ("registry-1.docker.io", "apache/kafka", "4.3.2"),
    "connect": ("quay.io", "debezium/connect", "3.7.1.Final"),
    "builder": ("registry-1.docker.io", "moby/buildkit", "v0.29.0"),
    "jre_alpine": (
        "registry-1.docker.io",
        "library/eclipse-temurin",
        "21.0.13.1_2-jre-alpine",
    ),
    "jre_ubi": (
        "registry-1.docker.io",
        "library/eclipse-temurin",
        "21.0.13.1_2-jre-ubi9-minimal",
    ),
}


class ImageError(ValueError):
    """外部応答や認証値を出さず、固定された失敗理由だけを返す。"""


class HttpsRedirect(HTTPRedirectHandler):
    """HTTPSのみ追跡し、別originにレジストリ認証を送らない。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        origin = urlsplit(newurl)
        if origin.scheme != "https" or origin.username or origin.password:
            raise ImageError("UNSAFE_REDIRECT")
        result = super().redirect_request(req, fp, code, msg, headers, newurl)
        if (
            result is not None
            and origin.netloc != urlsplit(req.full_url).netloc
        ):
            result.remove_header("Authorization")
        return result


def unique(pairs):
    """重複キーが異なる解釈を受ける余地をなくす。"""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ImageError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def reject_constant(_value):
    """JSON標準外の非有限値を受け付けない。"""
    raise ImageError("INVALID_JSON_CONSTANT")


def decode(raw):
    """構造をJSONオブジェクトに限定する。"""
    if len(raw) > MAX_JSON:
        raise ImageError("OVERSIZED_METADATA")
    data = json.loads(
        raw, object_pairs_hook=unique, parse_constant=reject_constant
    )
    if not isinstance(data, dict):
        raise ImageError("INVALID_METADATA")
    return data


def digest(raw):
    """バイト列のOCI識別子を計算する。"""
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def checked_digest(value):
    """SHA-256だけを許可し、パスに転用できる値を限定する。"""
    if not isinstance(value, str) or not DIGEST.fullmatch(value):
        raise ImageError("INVALID_DIGEST")
    return value


def descriptor(record):
    """OCI descriptorは識別子と厳密な非負サイズを持つ。"""
    ident = checked_digest(record["digest"])
    size = record["size"]
    if type(size) is not int or not 0 < size <= MAX_ARCHIVE:
        raise ImageError("INVALID_DESCRIPTOR_SIZE")
    if "urls" in record or "data" in record:
        raise ImageError("EXTERNAL_DESCRIPTOR")
    return ident, size


def docker_name(row):
    """Dockerfile向けに公開レジストリ名を一意に書く。"""
    if row["registry"] == "registry-1.docker.io":
        return row["repository"].removeprefix("library/")
    return "quay.io/" + row["repository"]


def source_lock():
    """上流とDockerfileの固定参照を一致させる。"""
    lock = decode((ASSETS / "images.lock.json").read_bytes())
    if lock.keys() != SOURCES.keys():
        raise ImageError("INVALID_SOURCE_LOCK")
    for key, (host, name, tag) in SOURCES.items():
        row = lock[key]
        fields = {"registry", "repository", "tag"}
        if key != "connect":
            fields.update(("index", "manifest", "config"))
        if set(row) != fields or (
            row["registry"],
            row["repository"],
            row["tag"],
        ) != (host, name, tag):
            raise ImageError("INVALID_SOURCE_LOCK")
        if (
            key != "connect"
            and len(
                {
                    checked_digest(row[k])
                    for k in ("index", "manifest", "config")
                }
            )
            != 3
        ):
            raise ImageError("INVALID_SOURCE_LOCK")
    runtime_files = {"kafka": "jre_alpine", "connect": "jre_ubi"}
    for name, jre in runtime_files.items():
        lines = (ASSETS / (name + ".Dockerfile")).read_text().splitlines()
        tags = []
        for key in (jre, name):
            row = lock[key]
            tag = f"FROM {docker_name(row)}:{row['tag']}"
            if "manifest" in row:
                tag += f"@{row['manifest']}"
            tags.append(tag)
        actual = [line for line in lines if line.upper().startswith("FROM ")]
        if actual != [tags[0] + " AS jre", tags[1]]:
            raise ImageError("DOCKERFILE_LOCK_MISMATCH")
    return lock


def request(url, token=None):
    """匿名公開レジストリから上限付きのメタデータを取得する。"""
    headers = {"Accept": ACCEPT}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    with build_opener(HttpsRedirect()).open(
        Request(url, headers=headers), timeout=30
    ) as response:
        data = response.read(MAX_JSON + 1)
        reported = response.headers.get("Docker-Content-Digest")
    if reported and reported != digest(data):
        raise ImageError("REGISTRY_DIGEST_MISMATCH")
    return data


def amd64_descriptor(index):
    """linux/amd64を一意に選択し、attestationと混同しない。"""
    choices = [
        item
        for item in index["manifests"]
        if item.get("platform", {}).get("os") == "linux"
        and item.get("platform", {}).get("architecture") == "amd64"
        and item["mediaType"] in (OCI_MANIFEST, DOCKER_MANIFEST)
    ]
    if len(choices) != 1:
        raise ImageError("AMBIGUOUS_PLATFORM")
    return descriptor(choices[0])


def exact_blob(raw, ident, size=None):
    """ヘッダーではなくデータ本体とサイズを検証する。"""
    if (size is not None and len(raw) != size) or digest(raw) != ident:
        raise ImageError("BLOB_DIGEST_MISMATCH")
    return decode(raw)


def platform(config):
    """manifestのラベルだけでなくconfigも実行プラットフォームに一致させる。"""
    if (config.get("os"), config.get("architecture")) != ("linux", "amd64"):
        raise ImageError("PLATFORM_MISMATCH")


def layer_diff_ids(config):
    """展開後レイヤーの順序とSHA-256をOCI configから取得する。"""
    rootfs = config["rootfs"]
    layers = rootfs["diff_ids"]
    if (
        rootfs["type"] != "layers"
        or not isinstance(layers, list)
        or not layers
    ):
        raise ImageError("INVALID_LAYERS")
    return [checked_digest(layer) for layer in layers]


def verify_source(name, row, fetch=request):
    """タグからindex・子manifest・configを照合し、観測した識別子を返す。"""
    host, repo, tag = SOURCES[name]
    token = None
    if host == "registry-1.docker.io":
        scope = urlencode(
            {
                "service": "registry.docker.io",
                "scope": f"repository:{repo}:pull",
            }
        )
        token = decode(fetch("https://auth.docker.io/token?" + scope))["token"]
    root = f"https://{host}/v2/{repo}"
    by_tag = fetch(f"{root}/manifests/{tag}", token)
    index_id = digest(by_tag)
    if "index" in row and index_id != row["index"]:
        raise ImageError("UPSTREAM_CHANGED")
    raw = fetch(f"{root}/manifests/{index_id}", token)
    index = exact_blob(raw, index_id)
    if index.get("schemaVersion") != 2 or index.get("mediaType") not in (
        OCI_INDEX,
        DOCKER_INDEX,
    ):
        raise ImageError("INVALID_INDEX")
    selected, size = amd64_descriptor(index)
    if "manifest" in row and selected != row["manifest"]:
        raise ImageError("PLATFORM_MISMATCH")
    raw = fetch(f"{root}/manifests/{selected}", token)
    image = exact_blob(raw, selected, size)
    if image.get("schemaVersion") != 2 or image.get("mediaType") not in (
        OCI_MANIFEST,
        DOCKER_MANIFEST,
    ):
        raise ImageError("INVALID_MANIFEST")
    config, size = descriptor(image["config"])
    if "config" in row and config != row["config"]:
        raise ImageError("CONFIG_MISMATCH")
    raw = fetch(f"{root}/blobs/{config}", token)
    config_data = exact_blob(raw, config, size)
    platform(config_data)
    evidence = {"index": index_id, "manifest": selected, "config": config}
    if "index" not in row:
        env = config_data["config"]["Env"]
        versions = [
            item for item in env if item.startswith("DEBEZIUM_VERSION=")
        ]
        if versions != [f"DEBEZIUM_VERSION={tag}"]:
            raise ImageError("DEBEZIUM_VERSION_MISMATCH")
        layers = layer_diff_ids(config_data)
        if len(layers) != len(image["layers"]):
            raise ImageError("INVALID_LAYERS")
        evidence["layers"] = layers
    return evidence


def hash_stream(stream, limit=MAX_ARCHIVE):
    """レイヤやアーカイブの全バイトを定量的に読んでハッシュ化する。"""
    hashed = hashlib.sha256()
    total = 0
    while block := stream.read(1024 * 1024):
        total += len(block)
        if total > limit:
            raise ImageError("OVERSIZED_BLOB")
        hashed.update(block)
    return "sha256:" + hashed.hexdigest(), total


def verify_archive(
    path,
    expected_sha,
    expected_manifest=None,
    expected_config=None,
    *,
    source_layers=None,
):
    """OCI tarを展開せず、外部固定SHAと単一amd64イメージを照合する。"""
    expected_sha = checked_digest("sha256:" + expected_sha)
    if expected_manifest is not None:
        checked_digest(expected_manifest)
    if expected_config is not None:
        checked_digest(expected_config)
    with path.open("rb") as stream:
        actual_sha, _ = hash_stream(stream)
        if actual_sha != expected_sha:
            raise ImageError("ARCHIVE_SHA256_MISMATCH")
        stream.seek(0)
        seen, metadata, blobs = set(), {}, {}
        # 作成側は非圧縮のOCI archiveを渡す。レイヤ自体の圧縮は保持する。
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            for member in archive:
                name = member.name.removeprefix("./")
                if name in seen or len(seen) >= 256:
                    raise ImageError("ARCHIVE_MEMBER_LIMIT")
                seen.add(name)
                if member.isdir() and name in (".", "blobs", "blobs/sha256"):
                    continue
                if not member.isfile() or member.issparse():
                    raise ImageError("INVALID_ARCHIVE_ENTRY")
                obj = archive.extractfile(member)
                if name in ("index.json", "oci-layout"):
                    metadata[name] = decode(obj.read(MAX_JSON + 1))
                    continue
                if not re.fullmatch(r"blobs/sha256/[0-9a-f]{64}", name):
                    raise ImageError("INVALID_ARCHIVE_PATH")
                ident = "sha256:" + name.rsplit("/", 1)[1]
                actual, size = hash_stream(obj)
                if actual != ident or size != member.size:
                    raise ImageError("BLOB_DIGEST_MISMATCH")
                blobs[ident] = (size, member)
        if metadata.get("oci-layout") != {"imageLayoutVersion": "1.0.0"}:
            raise ImageError("INVALID_OCI_LAYOUT")
        index = metadata["index.json"]
        manifests = index["manifests"]
        if index.get("schemaVersion") != 2 or len(manifests) != 1:
            raise ImageError("INVALID_ARCHIVE_INDEX")
        desc = manifests[0]
        if desc["mediaType"] != OCI_MANIFEST:
            raise ImageError("INVALID_MANIFEST")
        ident, size = descriptor(desc)
        if expected_manifest is not None and ident != expected_manifest:
            raise ImageError("ARCHIVE_MANIFEST_MISMATCH")
        if "platform" in desc:
            platform(desc["platform"])

        def verify_record(record):
            """索引の全参照を実在する検証済みblobに照合する。"""
            blob_id, length = descriptor(record)
            if blob_id not in blobs or blobs[blob_id][0] != length:
                raise ImageError("MISSING_ARCHIVE_BLOB")
            return blob_id, blobs[blob_id][1], length

        def read_record(record):
            """既に検証したmetadata blobを同じfdから読み直す。"""
            blob_id, member, length = verify_record(record)
            if length > MAX_JSON:
                raise ImageError("OVERSIZED_METADATA")
            stream.seek(member.offset_data)
            return exact_blob(stream.read(length), blob_id, length)

        image = read_record(desc)
        if (
            image.get("schemaVersion") != 2
            or image.get("mediaType") != OCI_MANIFEST
        ):
            raise ImageError("INVALID_MANIFEST")
        config_record = image["config"]
        if (
            config_record["mediaType"]
            != "application/vnd.oci.image.config.v1+json"
        ):
            raise ImageError("INVALID_CONFIG")
        if (
            expected_config is not None
            and config_record["digest"] != expected_config
        ):
            raise ImageError("ARCHIVE_CONFIG_MISMATCH")
        config = read_record(config_record)
        platform(config)
        layers = image["layers"]
        diffs = layer_diff_ids(config)
        if len(diffs) != len(layers):
            raise ImageError("INVALID_LAYERS")
        if source_layers is not None and (
            not source_layers
            or len(diffs) <= len(source_layers)
            or diffs[: len(source_layers)] != source_layers
        ):
            raise ImageError("ARCHIVE_SOURCE_LAYER_MISMATCH")
        referenced = {ident, config_record["digest"]}
        stream.seek(0)
        expanded = 0
        with tarfile.open(fileobj=stream, mode="r:") as archive:
            for layer, diff in zip(layers, diffs, strict=True):
                blob_id, member, length = verify_record(layer)
                kind = layer["mediaType"]
                if kind == "application/vnd.oci.image.layer.v1.tar":
                    uncompressed, size = blob_id, length
                elif kind == "application/vnd.oci.image.layer.v1.tar+gzip":
                    with (
                        archive.extractfile(member) as raw,
                        gzip.GzipFile(fileobj=raw) as unzipped,
                    ):
                        uncompressed, size = hash_stream(
                            unzipped, limit=MAX_EXPANDED_LAYER
                        )
                else:
                    raise ImageError("INVALID_LAYER_TYPE")
                expanded += size
                if size > MAX_EXPANDED_LAYER or expanded > MAX_ARCHIVE:
                    raise ImageError("OVERSIZED_BLOB")
                if uncompressed != diff:
                    raise ImageError("LAYER_DIFF_ID_MISMATCH")
                referenced.add(blob_id)
        if referenced != blobs.keys():
            raise ImageError("UNREFERENCED_ARCHIVE_BLOB")
    return expected_sha, ident, config_record["digest"]


def local_tag(kind, mode, config=None):
    """同じ固定namespaceをbuild・起動・照合に共用する。"""
    if kind not in {"kafka", "connect"}:
        raise ImageError("INVALID_LOCAL_IMAGE_KIND")
    alias = f"{STAGE3_NAMESPACE}-{kind}"
    if mode == "archive" and config is None:
        return alias + ":archive"
    if mode not in {"ci", "oci"} or config is None:
        raise ImageError("INVALID_LOCAL_IMAGE_MODE")
    value = checked_digest(config)[7:]
    return f"{alias}:{mode}-{value[:16] if mode == 'ci' else value}"


def inspect_command(reference):
    """Docker containerdの選択platformとdescriptorだけを取得する。"""
    return [
        "docker",
        "--context",
        "default",
        "image",
        "inspect",
        "--platform",
        "linux/amd64",
        "--format",
        "\n".join(
            "{{json ." + field + "}}"
            for field in ("Descriptor", "Id", "Os", "Architecture")
        ),
        reference,
    ]


def require_manifest_descriptor(descriptor, manifest, *, platform_required):
    """configや曖昧なトップレベルindexを実行manifestとして扱わない。"""
    checked_digest(manifest)
    if (
        not isinstance(descriptor, dict)
        or descriptor.get("mediaType") != OCI_MANIFEST
        or descriptor.get("digest") != manifest
    ):
        raise ImageError("ENGINE_MANIFEST_MISMATCH")
    declared = descriptor.get("platform")
    if declared is not None:
        platform(declared)
    elif platform_required:
        raise ImageError("ENGINE_MANIFEST_PLATFORM_UNVERIFIED")


def verify_engine_inspection(raw, manifest):
    """Mobyの選択platformのinspect出力と、確認済みmanifestを照合する。"""
    pieces = raw.strip().splitlines()
    if len(pieces) != 4:
        raise ImageError("ENGINE_MANIFEST_UNVERIFIED")
    descriptor, ident, os_name, architecture = (
        json.loads(
            line, object_pairs_hook=unique, parse_constant=reject_constant
        )
        for line in pieces
    )
    require_manifest_descriptor(descriptor, manifest, platform_required=False)
    if (ident, os_name, architecture) != (manifest, "linux", "amd64"):
        raise ImageError("ENGINE_IMAGE_ID_OR_PLATFORM_MISMATCH")


def main(argv=None):
    """差替え・pull・loadは行わず、固定識別子の検証だけを行う。"""
    cmd = argparse.ArgumentParser(description=__doc__)
    sub = cmd.add_subparsers(dest="method", required=True)
    registry = sub.add_parser("registry")
    registry.add_argument("--connect-receipt", type=Path)
    sub.add_parser("builder-ref")
    tag = sub.add_parser("local-tag")
    tag.add_argument("kind", choices=("kafka", "connect"))
    tag.add_argument("mode", choices=("archive", "ci", "oci"))
    tag.add_argument("--config")
    local = sub.add_parser("inspect-local")
    local.add_argument("reference")
    local.add_argument("--manifest", required=True)
    arc = sub.add_parser("archive")
    info = sub.add_parser("archive-info")
    for parse in (arc, info):
        parse.add_argument("path", type=Path)
        parse.add_argument("--sha256", required=True, help="tar SHA-256 (hex)")
    arc.add_argument(
        "--manifest", required=True, help="承認済みmanifest digest"
    )
    arc.add_argument("--config", required=True, help="承認済みconfig digest")
    info.add_argument("--source-receipt", type=Path)
    args = cmd.parse_args(argv)
    try:
        if args.method == "registry":
            for name, row in source_lock().items():
                evidence = verify_source(name, row)
                if name == "connect" and args.connect_receipt is not None:
                    with args.connect_receipt.open(
                        "x", encoding="utf-8"
                    ) as fd:
                        json.dump(evidence, fd, sort_keys=True)
                print(
                    name,
                    evidence["index"],
                    evidence["manifest"],
                    evidence["config"],
                    flush=True,
                )
        elif args.method == "builder-ref":
            row = source_lock()["builder"]
            print(f"{docker_name(row)}:{row['tag']}@{row['manifest']}")
        elif args.method == "local-tag":
            print(local_tag(args.kind, args.mode, args.config))
        elif args.method == "inspect-local":
            result = subprocess.run(
                inspect_command(args.reference),
                capture_output=True,
                timeout=15,
                check=False,
            )
            if result.returncode:
                raise ImageError("ENGINE_MANIFEST_UNVERIFIED")
            verify_engine_inspection(result.stdout, args.manifest)
        else:
            source_layers = None
            receipt = getattr(args, "source_receipt", None)
            if receipt is not None:
                evidence = decode(receipt.read_bytes())
                source_layers = evidence["layers"]
                if not isinstance(source_layers, list) or not source_layers:
                    raise ImageError("INVALID_SOURCE_RECEIPT")
                if evidence != verify_source(
                    "connect", source_lock()["connect"]
                ):
                    raise ImageError("CONNECT_TAG_CHANGED_DURING_BUILD")
            archive, manifest, config = verify_archive(
                args.path,
                args.sha256,
                getattr(args, "manifest", None),
                getattr(args, "config", None),
                source_layers=source_layers,
            )
            print(
                json.dumps(
                    {
                        "archive_sha256": archive,
                        "manifest": manifest,
                        "config": config,
                    },
                    sort_keys=True,
                )
            )
    except ImageError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except HTTPError as error:
        print(f"ERROR: REGISTRY_HTTP_{error.code}", file=sys.stderr)
        return 1
    except (
        OSError,
        EOFError,
        ValueError,
        KeyError,
        TypeError,
        RecursionError,
        tarfile.TarError,
        subprocess.TimeoutExpired,
        zlib.error,
    ):
        print("ERROR: IMAGE_VERIFICATION_FAILED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
