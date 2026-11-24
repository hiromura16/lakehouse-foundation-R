"""工場側のファイル確定と再送を一時ディレクトリで試験する。"""

import csv
import tempfile
import unittest
from hashlib import sha256
from io import StringIO
from pathlib import Path

from native.intake.spool import accept, canonical
from scripts.raw_file_input import ready_files


def product_row(name):
    """登録済みのERP列を使い、日本語を含む最小の検証用CSVを作る。"""
    fields = canonical("erp_products")["columns"]
    output = StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(fields)
    writer.writerow(
        [name if column == "product_name" else "demo" for column in fields]
    )
    return output.getvalue()


class IntakeSpoolTests(unittest.TestCase):
    """原本保全、送信元分離、完了印、型付き受入を検証する。"""

    def setUp(self):
        """一時フォルダへ、送信元と受け渡し先を用意する。"""
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name) / "landing"
        self.root.mkdir()
        self.source = Path(temp.name) / "factory.csv"

    def accept(self, sender, shipment, encoding="cp932"):
        """試験用の製品CSVを決めた送信元と配送IDで受け入れる。"""
        return accept(
            self.root, sender, shipment, self.source, "erp_products", encoding
        )

    def test_cp932_and_utf16_keep_original_and_normalized_bytes(self):
        """別送信元の同名入力を分け、日本語と原本SHAを保持する。"""
        text = product_row("プレス部品")
        first_bytes = text.encode("cp932")
        self.source.write_bytes(first_bytes)
        first = self.accept("factory-a", "batch-01")
        second_bytes = text.encode("utf-16")
        self.source.write_bytes(second_bytes)
        second = self.accept("factory-b", "batch-01", "utf-16")
        self.assertNotEqual(
            first["normalized_path"], second["normalized_path"]
        )
        self.assertEqual(
            first["original_sha256"], sha256(first_bytes).hexdigest()
        )
        self.assertEqual(
            second["original_sha256"], sha256(second_bytes).hexdigest()
        )
        for receipt in (first, second):
            path = self.root / receipt["normalized_path"]
            self.assertEqual(path.read_text(encoding="utf-8"), text)
        self.assertEqual(len(list(ready_files(self.root))), 6)

    def test_retry_and_conflicting_delivery(self):
        """同じ配送の再試行を許可し、内容の異なる上書きを拒否する。"""
        self.source.write_text(product_row("A"))
        first = self.accept("factory-a", "batch-01")
        self.assertEqual(self.accept("factory-a", "batch-01"), first)
        self.source.write_text(product_row("B"))
        with self.assertRaisesRegex(ValueError, "配送ID"):
            self.accept("factory-a", "batch-01")
        stored = self.root / first["original_path"]
        self.assertEqual(stored.read_text(), product_row("A"))
        self.assertEqual(len(list(ready_files(self.root))), 3)

    def test_bad_header_is_preserved_without_bronze_input(self):
        """未定義列の配送をRawと台帳に残し、業務表へは渡さない。"""
        self.source.write_bytes(b"unknown\nvalue\n")
        report = self.accept("factory-a", "batch-01")
        self.assertEqual(report["outcome"], "rejected")
        self.assertEqual(report["reason"], "SOURCE_CSV_HEADER")
        self.assertNotIn("normalized_path", report)
        self.assertEqual(len(list(ready_files(self.root))), 2)
        self.assertFalse((self.root / "csv").exists())
