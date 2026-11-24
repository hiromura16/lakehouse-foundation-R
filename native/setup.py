"""Create private Foundation R settings and a landing directory."""

import argparse
import os
import secrets
from pathlib import Path


def initialize(output: Path, landing: Path) -> None:
    """Write unique URL-safe credentials once with owner-only permissions.

    Docker mounts the directory read-only at the matching numerical UID/GID.
    The host writer publishes a file and its SHA-256 ready marker separately.
    Reusing an installation always means reusing the existing settings file.
    """
    if not landing.is_absolute() or not output.is_absolute():
        raise ValueError("Specify absolute settings and landing paths.")
    if any(ch in str(landing) for ch in ("'", "\n", "\r", "\\")):
        raise ValueError("Choose a landing path without quotes or escapes.")
    if not os.getuid() or not os.getgid():
        raise ValueError("Run setup as the unprivileged landing owner.")
    output.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    landing.mkdir(parents=True, mode=0o750, exist_ok=True)
    values = {
        "LANDING_DIR": f"'{landing}'",
        "LANDING_UID": str(os.getuid()),
        "LANDING_GID": str(os.getgid()),
        "CATALOG_DB_PASSWORD": secrets.token_hex(24),
        "CATALOG_ENCRYPTION_KEY": secrets.token_hex(32),
        "S3_ACCESS_KEY": secrets.token_hex(10),
        "S3_SECRET_KEY": secrets.token_hex(24),
        "WORKER_TOKEN": secrets.token_hex(32),
        "READER_TOKEN": secrets.token_hex(32),
        "GRAFANA_ADMIN_PASSWORD": secrets.token_hex(24),
    }
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            for key, value in values.items():
                file.write(f"{key}={value}\n")
            file.flush()
            os.fsync(file.fileno())
    except BaseException:
        output.unlink()
        raise


def main() -> None:
    """Report the settings path without disclosing any generated value."""
    parser = argparse.ArgumentParser(description=__doc__)
    config = Path.home() / ".config/lakehouse-foundation-r"
    parser.add_argument("--output", type=Path, default=config / "site.env")
    parser.add_argument("--landing", type=Path, default=config / "landing")
    args = parser.parse_args()
    try:
        initialize(args.output, args.landing)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Setup failed. {error}\n")
    print(f"Private settings created at {args.output}")


if __name__ == "__main__":
    main()
