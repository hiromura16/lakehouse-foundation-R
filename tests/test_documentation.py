"""文書の検出、生成範囲、リポジトリ内のナビゲーションを試す。

変更操作はすべて一時的なGit試験リポジトリで行う。
公開文書の原本と実行設定は維持する。
"""

import contextlib
import copy
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "documentation", ROOT / "scripts/documentation.py"
)
DOCUMENTATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DOCUMENTATION)


def entry(path, title):
    """公開文書間の関係を明示した、有効な編集用レコードを1件返す。"""
    return {
        "path": path,
        "title": title,
        "purpose": "Explain the fixture's role.",
        "audience": ["Reader"],
        "when_to_use": "Before checking the fixture.",
        "category": "Guides",
        "prerequisites": [],
        "next": [],
        "status": "active",
    }


class DocumentationTests(unittest.TestCase):
    """実際のGit規則で文書が検出されるよう、隔離したリポジトリを作る。"""

    def setUp(self):
        """2つの文書と、独立して生成したマニフェストを用意する。"""
        temporary = tempfile.TemporaryDirectory(prefix="documentation-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "docs").mkdir()
        self.write("README.md", "# Start\n\nKeep this body.\n")
        self.write("docs/guide.md", "# Guide\n\n[Start](../README.md)\n")
        self.index = {
            "README.md": entry("README.md", "Start"),
            "docs/guide.md": entry("docs/guide.md", "Guide"),
            DOCUMENTATION.MANIFEST: entry(
                DOCUMENTATION.MANIFEST, "Document manifest"
            ),
        }
        self.index["README.md"]["next"] = ["docs/guide.md"]
        self.index["docs/guide.md"]["prerequisites"] = ["README.md"]
        self.save_registry()
        self.write(
            DOCUMENTATION.MANIFEST,
            DOCUMENTATION.render_manifest(self.index),
        )
        subprocess.run(
            ["git", "init", "-q", str(self.root)],
            check=True,
            capture_output=True,
        )

    def write(self, path, text):
        """指定された試験ファイルだけを書き、必要なら親を作る。"""
        destination = self.root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8")

    def save_registry(self, documents=None):
        """試験用台帳を直列化する。異常系試験用には不正なレコードも許可する。"""
        self.write(
            DOCUMENTATION.REGISTRY,
            json.dumps(
                {
                    "schema_version": 1,
                    "documents": list(self.index.values())
                    if documents is None
                    else documents,
                }
            ),
        )

    def generate(self):
        """受入検査に先立ち、試験文書に目的ブロックを生成する。"""
        DOCUMENTATION.update_documents(self.root, self.index)

    def test_generation_preserves_body_and_is_idempotent(self):
        """挿入と後続の役割変更で、生成範囲外の文章を保つ。"""
        original = (
            '<p id="banner">Banner</p>\n\n# Start\n\n'
            "Keep **formatting**, 日本語, and trailing spaces.  \n\n"
            "```markdown\n[Example](missing.md)\n```\n"
        )
        output = DOCUMENTATION.with_purpose(original, "README.md", self.index)
        block = DOCUMENTATION.purpose_block("README.md", self.index)
        self.assertEqual(output.replace("\n" + block + "\n", ""), original)
        self.assertEqual(
            DOCUMENTATION.with_purpose(output, "README.md", self.index),
            output,
        )
        changed = copy.deepcopy(self.index)
        changed["README.md"]["purpose"] = "A revised specific role."
        revised = DOCUMENTATION.with_purpose(output, "README.md", changed)
        self.assertEqual(
            revised.split(DOCUMENTATION.START)[0],
            output.split(DOCUMENTATION.START)[0],
        )
        self.assertEqual(
            revised.split(DOCUMENTATION.END)[1],
            output.split(DOCUMENTATION.END)[1],
        )
        self.assertIn("A revised specific role.", revised)

    def test_purpose_uses_relative_prerequisite_and_next_links(self):
        """入れ子の生成文書からも、共通APIの参照先にリンクできる。"""
        with mock.patch.object(
            DOCUMENTATION,
            "load_index",
            return_value=self.index,
        ):
            block = DOCUMENTATION.purpose_block("docs/guide.md")
        self.assertIn("[Start](<../README.md>)", block)
        self.assertIn("(<DOCUMENTATION-MANIFEST.md>)", block)
        self.assertTrue(block.startswith(DOCUMENTATION.START))
        self.assertTrue(block.endswith(DOCUMENTATION.END))

    def test_existing_markers_must_be_unique_and_follow_first_h1(self):
        """壊れた、重複した、または位置のずれたブロックは上書きしない。"""
        block = DOCUMENTATION.purpose_block("README.md", self.index)
        malformed = [
            "# Start\n" + DOCUMENTATION.START,
            "# Start\n" + block + "\n" + block,
            block + "\n# Start\n",
            "# Start\n\nActual body.\n" + block,
            "# Start\n" + DOCUMENTATION.END + DOCUMENTATION.START,
        ]
        for text in malformed:
            with self.subTest(text=text[:25]), self.assertRaises(ValueError):
                DOCUMENTATION.with_purpose(text, "README.md", self.index)

    def test_discovery_respects_ignore_and_is_independent_of_cwd(self):
        """未追跡のMarkdownを含め、Gitで無視するローカルメモは除外する。"""
        self.write(".gitignore", "ignored/\n")
        self.write("ignored/private.md", "# Local only\n")
        self.write("docs/untracked note.md", "# New note\n")
        subprocess.run(
            ["git", "-C", str(self.root), "add", "README.md"],
            check=True,
        )
        previous = Path.cwd()
        try:
            os.chdir(self.root.parent)
            paths = DOCUMENTATION.markdown_paths(self.root)
        finally:
            os.chdir(previous)
        self.assertIn("README.md", paths)
        self.assertEqual(paths.count("README.md"), 1)
        self.assertIn("docs/untracked note.md", paths)
        self.assertNotIn("ignored/private.md", paths)

    def test_empty_checkout_discovers_no_phantom_document(self):
        """Gitの出力が空なら、空のパスを含む値ではなく空の一覧を返す。"""
        with tempfile.TemporaryDirectory(prefix="empty-docs-") as value:
            directory = Path(value)
            subprocess.run(
                ["git", "init", "-q", str(directory)],
                check=True,
                capture_output=True,
            )
            self.assertEqual(DOCUMENTATION.markdown_paths(directory), [])

    def test_first_h1_ignores_fenced_example_titles(self):
        """コード例の見出しで、目的ブロックがコードフェンス内に移動しない。"""
        text = "```markdown\n# Example\n```\n\n# Start\n\nBody.\n"
        result = DOCUMENTATION.with_purpose(text, "README.md", self.index)
        self.assertIn(
            "# Start\n\n" + DOCUMENTATION.START,
            result,
        )
        self.assertEqual(DOCUMENTATION.first_heading(text)[0], "Start")

    def test_coverage_detects_missing_and_unregistered_markdown(self):
        """台帳側と文書側の両方の不整合を、修正先が分かる診断にする。"""
        self.generate()
        self.write("docs/new.md", "# New\n")
        (self.root / "docs/guide.md").unlink()
        errors = DOCUMENTATION.check_documents(self.root, self.index)
        self.assertIn("Unregistered Markdown: docs/new.md", errors)
        self.assertIn(
            "Registered Markdown is missing: docs/guide.md",
            errors,
        )

    def test_generation_preflights_all_records_before_writing(self):
        """後のファイルが不正なら、先に確認したファイルも再生成しない。"""
        before = (self.root / "README.md").read_bytes()
        self.write("docs/guide.md", "# Wrong title\n")
        with self.assertRaisesRegex(ValueError, "title differs"):
            DOCUMENTATION.update_documents(self.root, self.index)
        self.assertEqual((self.root / "README.md").read_bytes(), before)

    def test_registry_rejects_duplicate_paths_and_unregistered_relations(self):
        """文書ごとに1件の登録と、移動先のパスの登録を必須にする。"""
        records = list(self.index.values())
        self.save_registry(records + [records[0]])
        with self.assertRaisesRegex(ValueError, "Duplicate document"):
            DOCUMENTATION.load_index(self.root)
        changed = copy.deepcopy(records)
        changed[0]["next"] = ["docs/not-registered.md"]
        self.save_registry(changed)
        with self.assertRaisesRegex(ValueError, "unregistered next"):
            DOCUMENTATION.load_index(self.root)

    def test_registry_rejects_invalid_paths_status_and_missing_fields(self):
        """不正なメタデータで、予定外の公開ファイルを編集しない。"""
        for field, value in (
            ("path", "../outside.md"),
            ("path", "/absolute.md"),
            ("status", "deployed"),
            ("audience", []),
            ("title", ""),
        ):
            changed = copy.deepcopy(list(self.index.values()))
            changed[0][field] = value
            self.save_registry(changed)
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    DOCUMENTATION.load_index(self.root)
        changed = copy.deepcopy(list(self.index.values()))
        del changed[0]["purpose"]
        self.save_registry(changed)
        with self.assertRaisesRegex(ValueError, "missing fields"):
            DOCUMENTATION.load_index(self.root)

    def test_registry_root_must_be_an_object(self):
        """JSONの配列、NULL、スカラーを入力エラーとして扱う。"""
        for value in ([], None, "text", 1):
            self.write(DOCUMENTATION.REGISTRY, json.dumps(value))
            with self.subTest(value=value), self.assertRaises(ValueError):
                DOCUMENTATION.load_index(self.root)

    def test_check_detects_stale_block_title_and_manifest_without_writes(self):
        """メタデータのずれや生成部の手編集は、読取専用の検査でエラーにする。"""
        self.generate()
        self.write(
            "README.md",
            (self.root / "README.md")
            .read_text()
            .replace(
                "# Start",
                "# Renamed",
                1,
            ),
        )
        self.index["docs/guide.md"]["purpose"] = "Updated role."
        self.write(DOCUMENTATION.MANIFEST, "# Document manifest\n")
        before = {name: (self.root / name).read_bytes() for name in self.index}
        errors = DOCUMENTATION.check_documents(self.root, self.index)
        self.assertTrue(any("title differs" in error for error in errors))
        self.assertTrue(any("missing or stale" in error for error in errors))
        self.assertTrue(any("generated manifest" in error for error in errors))
        self.assertEqual(
            before,
            {name: (self.root / name).read_bytes() for name in self.index},
        )

    def test_gfm_duplicate_headings_and_unicode_anchors(self):
        """句読点やCJK文字を含んでも、重複IDと明示したIDを扱える。"""
        self.write(
            "docs/guide.md",
            (
                "# Guide\n## Same heading\n## Same heading\n"
                "## Same heading-1\n## 日本語：見出し！\n"
                '<a id="explicit"></a>\n'
            ),
        )
        self.write(
            "README.md",
            (
                "# Start\n[First](docs/guide.md#same-heading)\n"
                "[Second](docs/guide.md#same-heading-1)\n"
                "[Collision](docs/guide.md#same-heading-1-1)\n"
                "[日本語](docs/guide.md#%E6%97%A5%E6%9C%AC%E8%AA%9E"
                "%E8%A6%8B%E5%87%BA%E3%81%97)\n"
                "[Explicit](docs/guide.md#explicit)\n"
            ),
        )
        self.assertEqual(
            DOCUMENTATION.check_links(
                self.root,
                ["README.md"],
            ),
            [],
        )
        with (self.root / "README.md").open("a") as stream:
            stream.write("[Bad](docs/guide.md#absent)\n")
        self.assertIn(
            "local anchor not found.",
            DOCUMENTATION.check_links(
                self.root,
                ["README.md"],
            )[0],
        )

    def test_html_references_images_and_fenced_examples(self):
        """コードフェンスの例を除外し、HTMLと画像の参照を検証する。"""
        self.write("docs/picture.svg", '<svg id="diagram"></svg>')
        self.write(
            "README.md",
            (
                '# Start\n<a href="docs/guide.md#guide">Guide</a>\n'
                '<img src="docs/picture.svg#diagram">\n'
                "![Image](docs/picture.svg)\n"
                "```markdown\n[Ignore](missing.md)\n"
                '<img src="also-missing.svg">\n```\n'
                '~~~html\n<a href="missing.html">No</a>\n~~~\n'
            ),
        )
        self.assertEqual(
            DOCUMENTATION.check_links(
                self.root,
                ["README.md"],
            ),
            [],
        )
        self.write("README.md", '# Start\n<img src="missing.svg">\n')
        self.assertEqual(
            DOCUMENTATION.check_links(
                self.root,
                ["README.md"],
            ),
            ["README.md:2: local link target not found."],
        )

    def test_reference_links_spaces_and_balanced_parentheses(self):
        """Markdownの一般的なリンク先で、空白や入れ子の括弧を保つ。"""
        self.write("docs/space name.md", "# Space\n")
        self.write("docs/file(1).md", "# Number\n")
        self.write(
            "README.md",
            (
                "# Start\n[Space](<docs/space name.md>)\n"
                "[Number](docs/file(1).md)\n[Ref][guide]\n"
                "[Root relative](/docs/guide.md#guide)\n"
                "[guide]: docs/guide.md#guide\n"
            ),
        )
        self.assertEqual(
            DOCUMENTATION.check_links(
                self.root,
                ["README.md"],
            ),
            [],
        )

    def test_heading_formatting_and_identifier_underscores(self):
        """GitHub見出しでは、インラインコードの識別子を残し、強調記号を除く。"""
        self.assertEqual(
            DOCUMENTATION.heading_slug(
                "This'll be a _Helpful_ Section About the Greek Letter Θ!"
            ),
            "thisll-be-a-helpful-section-about-the-greek-letter-θ",
        )
        self.assertEqual(
            DOCUMENTATION.heading_slug("Use `a_b` and __bold__"),
            "use-a_b-and-bold",
        )

    def test_local_escape_and_symlink_targets_are_rejected(self):
        """存在するパスでも、親参照やシンボリックリンクで境界外へ出られない。"""
        with tempfile.TemporaryDirectory(prefix="document-outside-") as value:
            outside = Path(value) / "outside.md"
            outside.write_text("# Outside\n")
            (self.root / "docs/linked.md").symlink_to(outside)
            self.write(
                "README.md",
                (
                    "# Start\n[Traversal](../outside.md)\n"
                    "[Encoded](%2e%2e/outside.md)\n"
                    "[Symlink](docs/linked.md#outside)\n"
                ),
            )
            errors = DOCUMENTATION.check_links(self.root, ["README.md"])
            self.assertEqual(len(errors), 3)
            self.assertTrue(
                all("escapes repository" in error for error in errors)
            )

    def test_directory_anchor_and_document_symlinks_cannot_escape(self):
        """ディレクトリのREADMEとMarkdown原本に、同じルート境界を適用する。"""
        with tempfile.TemporaryDirectory(prefix="outside-anchor-") as value:
            outside = Path(value) / "outside.md"
            outside.write_text("# Outside\n")
            (self.root / "docs/subdir").mkdir()
            (self.root / "docs/subdir/README.md").symlink_to(outside)
            (self.root / "docs/source.md").symlink_to(outside)
            self.write("README.md", "# Start\n[Dir](docs/subdir/#outside)\n")
            errors = DOCUMENTATION.check_links(
                self.root,
                ["README.md", "docs/source.md"],
            )
            self.assertEqual(len(errors), 2)
            self.assertTrue(
                all("escapes repository" in error for error in errors)
            )

    def test_cli_from_other_cwd_generates_then_checks_without_mutation(self):
        """実際の起動方法で、対象チェックアウトの外からも実行できる。"""
        command = [
            sys.executable,
            str(ROOT / "scripts/documentation.py"),
            "--root",
            str(self.root),
        ]
        generated = subprocess.run(
            command,
            cwd=self.root.parent,
            capture_output=True,
            text=True,
        )
        self.assertEqual(generated.returncode, 0, generated.stdout)
        before = {
            name: (self.root / name).stat().st_mtime_ns for name in self.index
        }
        checked = subprocess.run(
            command + ["--check"],
            cwd=self.root.parent,
            capture_output=True,
            text=True,
        )
        self.assertEqual(checked.returncode, 0, checked.stdout)
        self.assertIn("3 Markdown files", checked.stdout)
        self.assertEqual(
            before,
            {
                name: (self.root / name).stat().st_mtime_ns
                for name in self.index
            },
        )

    def test_cli_invalid_registry_does_not_traceback_or_reveal_root(self):
        """不正な入力では、OSパスを含めず簡潔な公開用診断を返す。"""
        self.write(DOCUMENTATION.REGISTRY, "{invalid}")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = DOCUMENTATION.main(["--root", str(self.root), "--check"])
        self.assertEqual(result, 1)
        self.assertNotIn("Traceback", output.getvalue())
        self.assertNotIn(str(self.root), output.getvalue())

    def test_generated_manifest_has_complete_category_and_reader_views(self):
        """生成した索引に、実際のリンクと編集規則が含まれる。"""
        rendered = DOCUMENTATION.render_manifest(self.index)
        self.assertIn("### Guides", rendered)
        self.assertIn("### Reader", rendered)
        self.assertIn("[Guide](<guide.md>)", rendered)
        self.assertIn("scripts/documentation.py --check", rendered)
        self.generate()
        self.assertEqual(
            DOCUMENTATION.check_documents(self.root, self.index),
            [],
        )


if __name__ == "__main__":
    unittest.main()
