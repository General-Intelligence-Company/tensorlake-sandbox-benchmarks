"""Pinned tools installed once before prepared-image measurements."""

INSTALL = """set -eu
if command -v sudo >/dev/null; then SUDO=sudo; else SUDO=; fi
$SUDO mkdir -p /opt/bench /workspace /mnt/library /var/log/bench
if ! test -x /opt/bench-venv/bin/python || ! command -v s3fs >/dev/null; then
  $SUDO env DEBIAN_FRONTEND=noninteractive apt-get update -qq
  $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv python3 git s3fs curl
  if ! command -v node >/dev/null || ! command -v npm >/dev/null; then
    $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs npm
  fi
  $SUDO python3 -m venv /opt/bench-venv
  $SUDO /opt/bench-venv/bin/pip -q install playwright==1.58.0
  $SUDO /opt/bench-venv/bin/python -m playwright install --with-deps chromium
fi
$SUDO /opt/bench-venv/bin/python -c 'from playwright.sync_api import sync_playwright; from pathlib import Path; p=sync_playwright().start(); t=Path("/root/.agent-browser/browsers/portable"); t.mkdir(parents=True,exist_ok=True); c=t/"chrome"; c.symlink_to(p.chromium.executable_path) if not c.exists() else None; p.stop()'
$SUDO chmod 1777 /workspace
"""
