"""固定した公開版のIcebergファイルを、倉庫のS3メタデータで確認する。"""

from collections import Counter
from hashlib import sha256
from urllib.parse import urlsplit

from botocore.exceptions import BotoCoreError, ClientError

MAX_METADATA_BYTES = 32 * 1024 * 1024
MAX_ISSUE_DETAILS = 16


class InventoryStopped(Exception):
    """上限、またはS3への照合失敗で完全な確認を継続できない。"""


class ObjectInventory:
    """同じキーの照合を一度にし、問題箇所は位置のハッシュで識別する。"""

    def __init__(self, client, bucket, limit):
        self.client = client
        self.bucket = bucket
        self.limit = limit
        self.seen = {}
        self.kinds = Counter()
        self.issue_counts = Counter()
        self.issue_details = []

    def issue(self, table, kind, code, location=None):
        """頻度を集計し、調査に使う最初の数件だけを応答へ載せる。"""
        self.issue_counts[code] += 1
        if len(self.issue_details) < MAX_ISSUE_DETAILS:
            item = {"table": table, "kind": kind, "code": code}
            if location is not None:
                item["location_sha256"] = sha256(location.encode()).hexdigest()
            self.issue_details.append(item)

    def head(self, table, kind, location):
        """倉庫バケットだけを調べ、HEAD応答の404と権限エラーを区別する。"""
        uri = urlsplit(location)
        if (
            uri.scheme not in {"s3", "s3a"}
            or uri.netloc != self.bucket
            or not uri.path.removeprefix("/")
            or uri.query
            or uri.fragment
        ):
            self.issue(table, kind, "UNEXPECTED_LOCATION", location)
            raise InventoryStopped
        try:
            response = self.client.head_object(
                Bucket=self.bucket, Key=uri.path.removeprefix("/")
            )
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode"
            )
            if status == 404:
                self.issue(table, kind, "NOT_FOUND", location)
                return None
            self.issue(table, kind, "HEAD_FAILED", location)
            raise InventoryStopped from None
        except BotoCoreError:
            self.issue(table, kind, "HEAD_FAILED", location)
            raise InventoryStopped from None
        return response["ContentLength"]

    def confirm(self, table, kind, location, expected=None):
        """同じファイルを再利用しても、新しい期待サイズは照合する。"""
        if location not in self.seen:
            if len(self.seen) == self.limit:
                self.issue(table, kind, "OBJECT_LIMIT")
                raise InventoryStopped
            self.seen[location] = self.head(table, kind, location)
            self.kinds[kind] += 1
        actual = self.seen[location]
        if actual is None:
            return False
        if actual <= 0 or (expected is not None and expected != actual):
            self.issue(table, kind, "SIZE_MISMATCH", location)
            return False
        if (
            kind in {"manifest_list", "manifest"}
            and actual > MAX_METADATA_BYTES
        ):
            self.issue(table, kind, "MANIFEST_SIZE_LIMIT", location)
            return False
        return True


def inspect_table(inventory, catalog, name, snapshot_id):
    """リスト・マニフェストを順に確かめ、現在版のデータと削除ファイルを読む。"""
    table = catalog.load_table(name)
    inventory.confirm(name, "table_metadata", table.metadata_location)
    if snapshot_id is None:
        return True
    snapshot = table.snapshot_by_id(int(snapshot_id))
    if snapshot is None:
        inventory.issue(name, "snapshot", "SNAPSHOT_NOT_FOUND")
        return False
    if not snapshot.manifest_list or not inventory.confirm(
        name, "manifest_list", snapshot.manifest_list
    ):
        if not snapshot.manifest_list:
            inventory.issue(name, "manifest_list", "MANIFEST_LIST_NOT_FOUND")
        return False
    complete = True
    for manifest in snapshot.manifests(table.io):
        if not inventory.confirm(
            name, "manifest", manifest.manifest_path, manifest.manifest_length
        ):
            complete = False
            continue
        # 有効なデータと削除ファイルを対象にする。
        for entry in manifest.fetch_manifest_entry(table.io):
            item = entry.data_file
            kind = "data" if item.content.name == "DATA" else "delete"
            inventory.confirm(
                name, kind, item.file_path, item.file_size_in_bytes
            )
    return complete


def storage_report(catalog, findings, prefix, *, client, bucket, maximum):
    """選択した版だけを照合し、未到達・不明と確認済みを分けて返す。"""
    inventory = ObjectInventory(client, bucket, maximum)
    matches = [
        *findings["references"],
        {
            "table": prefix + "gold._release",
            "snapshot_id": findings["release_snapshot_id"],
            "metadata_available": True,
        },
    ]
    complete = True
    for match in matches:
        name = match["table"]
        if not match["metadata_available"]:
            inventory.issue(name, "snapshot", "SNAPSHOT_NOT_FOUND")
            complete = False
            continue
        try:
            complete = (
                inspect_table(inventory, catalog, name, match["snapshot_id"])
                and complete
            )
        except InventoryStopped:
            complete = False
            break
        except Exception:
            # Avroの破損等でも、上流由来のファイルパスや応答本文は表示しない。
            inventory.issue(name, "metadata", "METADATA_READ_FAILED")
            complete = False
    return {
        "enumeration_complete": complete,
        "all_objects_available": complete and not inventory.issue_counts,
        "unique_objects_examined": len(inventory.seen),
        "object_counts": dict(inventory.kinds),
        "issue_counts": dict(inventory.issue_counts),
        "sample_issues": inventory.issue_details,
    }
