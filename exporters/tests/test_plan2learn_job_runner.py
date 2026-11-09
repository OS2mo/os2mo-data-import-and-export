import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("export_status", [0, 7])
@pytest.mark.parametrize("term", ["xterm", "dumb"])
def test_plan2learn_job_runner(tmp_path: Path, export_status: int, term: str) -> None:
    root = Path(__file__).resolve().parents[2]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    settings_python = bin_dir / "python"
    settings_python.write_text("#!/bin/sh\nexit 0\n")
    settings_python.chmod(0o755)
    job_python = bin_dir / "python3"
    job_python.write_text(
        '#!/bin/sh\n'
        'case "$1" in\n'
        '  */plan2learn.py) exit "$EXPORT_STATUS" ;;\n'
        '  */ship_files.py) touch "$SHIPPED_MARKER" ;;\n'
        '  *) exit 99 ;;\n'
        'esac\n'
    )
    job_python.chmod(0o755)
    systemd_cat = bin_dir / "systemd-cat"
    systemd_cat.write_text("#!/bin/sh\ncat\n")
    systemd_cat.chmod(0o755)
    curl = bin_dir / "curl"
    curl.write_text('#!/bin/sh\ncat >> "$METRICS_FILE"\n')
    curl.chmod(0o755)
    marker = tmp_path / "shipped"
    metrics = tmp_path / "metrics"
    env = os.environ.copy()
    env.update(
        DIPEXAR=str(root),
        VENV=str(tmp_path),
        RUN_DB_BACKUP="false",
        CRON_LOG_PROM_API="http://metrics.invalid",
        TERM=term,
        PATH=f"{bin_dir}{os.pathsep}{env['PATH']}",
        EXPORT_STATUS=str(export_status),
        SHIPPED_MARKER=str(marker),
        METRICS_FILE=str(metrics),
    )
    result = subprocess.run(
        ["bash", "-c", 'source "$DIPEXAR/tools/job-runner.sh"; run-job exports_plan2learn'],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == export_status
    assert marker.exists() == (export_status == 0)
    assert f"mo_return_code {export_status}\n" in metrics.read_text()
    if export_status:
        assert "ERROR: Plan2Learn export failed" in result.stdout + result.stderr
