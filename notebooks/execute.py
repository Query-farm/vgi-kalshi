"""Execute examples in fresh local kernels, publishing notebook outputs only on success."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic

import nbformat
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager
from nbclient import NotebookClient


def main() -> None:
    """Run the default notebook or separate copies with visible parameter overrides."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("notebooks", nargs="*", type=Path)
    parser.add_argument("--parameters", default="{}", help="JSON object overriding visible parameters.")
    parser.add_argument("--output-dir", type=Path, help="Directory for separate executed copies.")
    parser.add_argument("--timeout", type=int, default=180, help="Maximum seconds per cell.")
    args = parser.parse_args()
    overrides = json.loads(args.parameters)
    if not isinstance(overrides, dict):
        parser.error("--parameters must be a JSON object")
    if overrides and args.output_dir is None:
        parser.error("Parameter overrides require --output-dir to preserve successful defaults.")
    directory = Path(__file__).resolve().parent
    paths = [p.resolve() for p in args.notebooks] or sorted(directory.glob("[0-9][0-9]_*.ipynb"))
    destination_dir = args.output_dir.resolve() if args.output_dir else None
    if overrides and any(destination_dir / p.name == p for p in paths):
        parser.error("Override output must be a separate destination, not the source notebook.")
    os.environ["PLOOMBER_STATS_ENABLED"] = "false"

    with TemporaryDirectory(prefix="vgi-kalshi-kernel-") as temporary:
        kernel_dir = Path(temporary) / "kalshi-examples"
        kernel_dir.mkdir()
        (kernel_dir / "kernel.json").write_text(
            json.dumps(
                {
                    "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                    "display_name": "Python (vgi-kalshi notebooks)",
                    "language": "python",
                }
            )
        )
        for path in paths:
            notebook = nbformat.read(path, as_version=4)
            nbformat.validate(notebook)
            parameter_cell = next(c for c in notebook.cells if "parameters" in c.metadata.get("tags", []))
            names = {
                node.targets[0].id
                for node in ast.parse(parameter_cell.source).body
                if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
            }
            if unknown := overrides.keys() - names:
                raise ValueError(f"Unknown parameters: {sorted(unknown)}")
            for key, value in overrides.items():
                parameter_cell.source = re.sub(
                    rf"^{re.escape(key)}\s*=.*$",
                    lambda _, name=key, replacement=value: f"{name} = {replacement!r}",
                    parameter_cell.source,
                    flags=re.MULTILINE,
                )
            manager = KernelManager(
                kernel_name="kalshi-examples",
                kernel_spec_manager=KernelSpecManager(kernel_dirs=[temporary]),
            )
            print(f"Running {path.name} ...", flush=True)
            start = monotonic()
            client = NotebookClient(
                notebook,
                km=manager,
                timeout=args.timeout,
                allow_errors=False,
                resources={"metadata": {"path": str(directory)}},
            )
            try:
                client.execute(cleanup_kc=True)
            except Exception:
                print("FAILED; the previous successful notebook has not been changed.", flush=True)
                raise
            nbformat.validate(notebook)
            destination = destination_dir / path.name if destination_dir else path
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination_dir and destination_dir != directory:
                shutil.copytree(directory / "assets", destination_dir / "assets", dirs_exist_ok=True)
                shutil.copytree(directory / "fixtures", destination_dir / "fixtures", dirs_exist_ok=True)
                audit = directory / "artifacts/request-audit.json"
                if audit.exists():
                    (destination_dir / "artifacts").mkdir(exist_ok=True)
                    shutil.copy2(audit, destination_dir / "artifacts/request-audit.json")
                for companion in (
                    "README.md",
                    "replay_source.py",
                    "STANDARD.md",
                    "execute.py",
                    "pyproject.toml",
                    "uv.lock",
                ):
                    shutil.copy2(directory / companion, destination_dir / companion)
                (destination_dir / "RUN.json").write_text(
                    json.dumps({"source": str(path), "parameters": overrides}, indent=2) + "\n"
                )
            staged = destination.with_suffix(".ipynb.tmp")
            nbformat.write(notebook, staged)
            staged.replace(destination)
            plots = sum(
                "image/png" in o.get("data", {}) for c in notebook.cells for o in c.get("outputs", [])
            )
            print(f"OK: {plots} charts, {monotonic() - start:.1f}s; saved {destination}", flush=True)


if __name__ == "__main__":
    main()
