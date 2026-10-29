#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
development_root="$repo_root/.development"
hermes_checkout="$development_root/hermes-agent"
venv="$repo_root/.venv/tests314"
expected_origin="https://github.com/NousResearch/hermes-agent.git"

mkdir -p "$development_root"
if [[ ! -e "$hermes_checkout" ]]; then
  git clone --filter=blob:none "$expected_origin" "$hermes_checkout"
elif [[ ! -d "$hermes_checkout/.git" ]]; then
  echo "$hermes_checkout exists but is not a Git checkout" >&2
  exit 1
fi

origin="$(git -C "$hermes_checkout" remote get-url origin)"
if [[ "$origin" != "$expected_origin" ]]; then
  echo "Development Hermes origin must be $expected_origin; got $origin" >&2
  exit 1
fi
if [[ -n "$(git -C "$hermes_checkout" status --porcelain)" ]]; then
  echo "Development Hermes checkout has local changes; refusing to replace them" >&2
  exit 1
fi

git -C "$hermes_checkout" fetch origin main
git -C "$hermes_checkout" switch --detach origin/main

# Build through Hermes PM without touching the gateway's selected environment.
# A fresh destination also avoids silently clearing an existing developer venv.
python="$(uv python find 3.14)"
mkdir -p "$repo_root/.venv/pm-home"
pm_python="$(HOME="$repo_root/.venv/pm-home" \
  HERMES_HOME="$repo_root/.venv/pm-home/.hermes" \
  PYTHONPATH="$hermes_checkout" "$python" -c \
  'from pm.runtime import runtime_python; print(runtime_python())')"
HOME="$repo_root/.venv/pm-home" \
  HERMES_HOME="$repo_root/.venv/pm-home/.hermes" \
  PYTHONPATH="$hermes_checkout" \
  "$pm_python" - "$repo_root" "$hermes_checkout" "$venv" <<'PY'
from pathlib import Path
import platform
import shutil
import sys
import tomllib

import tomli_w

import pm
from pm.workspace import _generate_pyproject

repo, hermes, destination = map(Path, sys.argv[1:])
workspace = repo / ".venv" / "test-project314"
if destination.exists() or workspace.exists():
    raise SystemExit("Test environment or build inputs already exist; remove the disposable outputs before rebuilding")
# Match production's core + native-plugin dependency union. Hermes now supports
# editable source installs only; building a Hermes wheel is intentionally refused.
_generate_pyproject(
    [repo / "hermes_realtime"], workspace, source=hermes, python=Path(sys.executable),
)
# This disposable environment runs only on this host. Preserve Hermes' Python
# constraints without resolving unsupported targets such as Android, where the
# plugin's native dependencies have no wheels.
manifest = workspace / "pyproject.toml"
document = tomllib.loads(manifest.read_text(encoding="utf-8"))
uv = document.setdefault("tool", {}).setdefault("uv", {})
environments = uv.get("environments", [])
if isinstance(environments, str):
    environments = [environments]
host = f"sys_platform == {sys.platform!r} and platform_machine == {platform.machine()!r}"
uv["environments"] = [f"({marker}) and {host}" for marker in environments] or [host]
manifest.write_text(tomli_w.dumps(document), encoding="utf-8")
shutil.copy2(hermes / "uv.lock", workspace / "uv.lock")
python = pm.build_environment(
    source=workspace, out=destination, python=Path(sys.executable),
    cache=repo / ".venv" / "pm-cache", extras=["messaging", "audio-io", "web"],
    groups=["dev", "test"], frozen=False, explicit=True,
)
print(python)
PY

echo "Vanilla Hermes test environment is ready at $venv"
