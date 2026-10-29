"""Native plugin discovery and multi-profile PM admission use the real host."""

from pathlib import Path
import os
import shutil
import subprocess
import sys
import tomllib
from ruamel.yaml import YAML
from packaging.requirements import Requirement

from pm.plugin_declarations import read_python_declaration
from pm.workspace import _workspace_member


ROOT = Path(__file__).resolve().parents[1]
NATIVE = ROOT / "hermes_realtime"


def test_shared_dependencies_accept_the_hosts_selected_versions():
    import gateway

    host = Path(gateway.__file__).resolve().parents[1]
    project = tomllib.loads((host / "pyproject.toml").read_text())["project"]
    declared = list(project["dependencies"])
    for extra in ("audio-io", "messaging"):
        declared.extend(project["optional-dependencies"].get(extra, []))
    plugin = {
        requirement.name.lower(): requirement
        for requirement in map(Requirement, read_python_declaration(NATIVE).install_requirements)
    }
    for requirement in map(Requirement, declared):
        if requirement.marker and not requirement.marker.evaluate():
            continue
        shared = plugin.get(requirement.name.lower())
        if shared is None:
            continue
        for specifier in requirement.specifier:
            if specifier.operator == "==" and "*" not in specifier.version:
                assert shared.specifier.contains(specifier.version), (
                    f"{shared} rejects the Hermes host's {requirement}"
                )


def test_native_dependencies_admit_the_current_python_and_match_pip():
    declaration = read_python_declaration(NATIVE)
    distribution = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    native = tomllib.loads((NATIVE / "pyproject.toml").read_text())["project"]
    manifest = YAML(typ="safe").load((NATIVE / "plugin.yaml").read_text())

    assert distribution["version"] == native["version"] == manifest["version"]
    assert declaration.installs_packages
    assert declaration.python_error("3.14.7") is None
    assert set(declaration.install_requirements) == set(distribution["dependencies"])


def test_two_profile_copies_have_distinct_pm_members(tmp_path):
    workspace = tmp_path / "workspace"
    members = []
    for profile in ("default", "mira"):
        source = tmp_path / profile / "plugins" / "realtime"
        shutil.copytree(NATIVE, source, ignore=shutil.ignore_patterns("__pycache__"))
        member = _workspace_member(source, workspace, identity=source)
        members.append(tomllib.loads((member / "pyproject.toml").read_text()))

    assert members[0]["project"]["name"] != members[1]["project"]["name"]
    assert members[0]["project"]["dependencies"] == members[1]["project"]["dependencies"]
    assert all("build-system" not in member for member in members)


def test_directory_install_registers_both_platforms_without_pip_entrypoint(tmp_path):
    plugin = tmp_path / "plugins" / "realtime"
    shutil.copytree(NATIVE, plugin, ignore=shutil.ignore_patterns("__pycache__"))
    probe = """
from pathlib import Path
import os
from hermes_cli.plugins import PluginManager
from hermes_cli.plugins_discovery import scan_directory
from gateway.platform_registry import platform_registry

home = Path(os.environ['HERMES_HOME'])
manifests = scan_directory(home / 'plugins', 'user')
assert len(manifests) == 1 and manifests[0].name == 'realtime'
manager = PluginManager()
try:
    manager._load_plugin(manifests[0])
    loaded = manager._plugins['realtime']
    assert loaded.error is None, loaded.error
    assert loaded.module.__file__ == str(home / 'plugins/realtime/__init__.py')
    for name in ('livekit', 'realtime'):
        entry = platform_registry.get(name)
        assert entry is not None, name
        assert entry.check_fn(), name
finally:
    manager.unload()
"""
    result = subprocess.run(
        [sys.executable, "-c", probe],
        env={**os.environ, "HERMES_HOME": str(tmp_path)},
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
