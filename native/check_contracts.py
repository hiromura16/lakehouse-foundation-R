"""Verify the hashes of the copied Foundation business contracts."""

import json
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    """Compare the committed manifest with the exact local source contents."""
    path = ROOT / "contracts/native/foundation-source.json"
    manifest = json.loads(path.read_text())
    files = manifest["source_sha256"]
    for name, recorded in files.items():
        source = (ROOT / name).resolve()
        if not source.is_relative_to(ROOT):
            raise SystemExit(
                "Contract manifest contains a path outside the repo."
            )
        if sha256(source.read_bytes()).hexdigest() != recorded:
            raise SystemExit(f"Contract content changed: {name}")
    print(f"Verified {len(files)} Foundation contract files.")


if __name__ == "__main__":
    main()
