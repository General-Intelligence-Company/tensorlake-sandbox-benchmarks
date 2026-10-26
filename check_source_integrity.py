"""Check the public source manifest without Git history or cloud calls."""

import hashlib
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / "source-manifest.json").read_text())
    mismatches = []
    for name, expected in manifest["files"].items():
        path = root / name
        actual = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        if actual != expected:
            mismatches.append(name)
    print(json.dumps({"files_checked": len(manifest["files"]), "mismatches": mismatches}))
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
