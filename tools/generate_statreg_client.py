"""Generate the read-only Statreg client from an untracked local OpenAPI spec."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "src/fagfunksjoner/api/statistikkregisteret/_generated"
OPERATIONS = {
    "/contacts": "get_contacts",
    "/statistics": "get_statistics",
    "/statistics/{shortname}": "get_statistic",
    "/statistics/{shortname}/variants/{id}/releases": "get_variant_releases",
    "/releases": "get_releases",
    "/releases/{id}": "get_release",
}


def normalize(value: Any) -> Any:
    """Translate legacy nullable keywords used in this OpenAPI 3.1 document."""
    if isinstance(value, list):
        return [normalize(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: normalize(item) for key, item in value.items() if key != "nullable"}
    if value.get("nullable") and isinstance(result.get("type"), str):
        result["type"] = [result["type"], "null"]
    return result


def main() -> None:
    """Generate a reproducible client, leaving handwritten utilities untouched."""
    if version("openapi-python-client") != "0.29.1":
        raise RuntimeError(
            "Install the pinned codegen dependency group before regenerating"
        )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spec", type=Path, default=ROOT / "src/fagfunksjoner/api/openapi.yaml"
    )
    parser.add_argument("--upstream-revision", default=None)
    args = parser.parse_args()
    content = args.spec.read_bytes()
    spec = normalize(YAML(typ="safe").load(content))
    # Hoist the inherited inline status object to avoid duplicate model names.
    schemas = spec["components"]["schemas"]
    schemas["Statistic_status"] = schemas["Statistic"]["properties"]["status"]
    schemas["Statistic"]["properties"]["status"] = {
        "$ref": "#/components/schemas/Statistic_status"
    }

    def flatten(value: Any) -> Any:
        if isinstance(value, list):
            return [flatten(item) for item in value]
        if not isinstance(value, dict):
            return value
        result: dict[str, Any] = {}
        for part in value.get("allOf", []):
            if "$ref" in part:
                part = schemas[part["$ref"].rsplit("/", 1)[1]]
            part = flatten(part)
            result.update(
                {
                    key: item
                    for key, item in part.items()
                    if key not in {"properties", "required"}
                }
            )
            result.setdefault("properties", {}).update(part.get("properties", {}))
            result.setdefault("required", []).extend(part.get("required", []))
        for key, item in value.items():
            if key == "allOf":
                continue
            if key == "properties":
                result.setdefault(key, {}).update(flatten(item))
            elif key == "required":
                result.setdefault(key, []).extend(item)
            else:
                result[key] = flatten(item)
        if "required" in result:
            result["required"] = list(dict.fromkeys(result["required"]))
        return result

    # The generator otherwise drops sibling properties next to allOf.
    schemas = {name: flatten(schema) for name, schema in schemas.items()}
    spec["components"]["schemas"] = schemas
    spec["paths"] = {
        path: {"get": {**spec["paths"][path]["get"], "operationId": operation}}
        for path, operation in OPERATIONS.items()
    }
    # Keep only components reachable from the selected operations.
    components = spec.pop("components")
    selected: dict[str, dict[str, Any]] = {}

    def visit(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            ref = value.get("$ref", "")
            if ref.startswith("#/components/"):
                _, _, kind, name = ref.split("/")
                bucket = selected.setdefault(kind, {})
                if name not in bucket:
                    bucket[name] = components[kind][name]
                    visit(bucket[name])
            for item in value.values():
                visit(item)

    # Every selected GET explicitly has security: [].
    spec.pop("security", None)
    visit(spec)
    spec["components"] = selected
    with tempfile.TemporaryDirectory(prefix="statreg-generate-", dir=ROOT) as temporary:
        work = Path(temporary)
        input_path = work / "openapi.json"
        input_path.write_text(json.dumps(spec), encoding="utf-8")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "openapi_python_client",
                "generate",
                "--path",
                str(input_path),
                "--config",
                str(ROOT / "tools/statreg-client.yaml"),
                "--custom-template-path",
                str(ROOT / "tools/statreg-templates"),
                "--meta",
                "none",
                "--output-path",
                str(work / "client"),
            ],
            check=True,
        )
        generated = work / "client"
        if not generated.is_dir():
            raise RuntimeError("Generator did not produce the expected package")
        client_file = generated / "client.py"
        client_file.write_text(
            client_file.read_text(encoding="utf-8").replace(
                "from typing import Any, Self",
                "from typing import Any\nfrom typing_extensions import Self",
            ),
            encoding="utf-8",
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "ruff",
                "check",
                "--isolated",
                "--select",
                "F,I",
                "--fix",
                str(generated),
            ],
            check=True,
        )
        subprocess.run(
            [sys.executable, "-m", "ruff", "format", "--isolated", str(generated)],
            check=True,
        )
        if (
            TARGET.resolve().parent
            != (ROOT / "src/fagfunksjoner/api/statistikkregisteret").resolve()
        ):
            raise RuntimeError("Generated target escaped the package directory")
        if TARGET.exists():
            shutil.rmtree(TARGET)
        shutil.copytree(generated, TARGET)
    provenance = {
        "generator": "openapi-python-client==0.29.1",
        "spec_sha256": hashlib.sha256(content).hexdigest(),
        "upstream_url": "https://raw.githubusercontent.com/statisticsnorway/ssbno-statreg/refs/heads/main/api-types/openapi/openapi.yaml",
        "upstream_revision": args.upstream_revision,
        "operations": OPERATIONS,
        "normalization": "Nullable type unions; flatten object allOf; shared statistic status; typing_extensions.Self and UTC Z parsing for Python 3.10; date fields accept observed timestamp input; read-only subset",
    }
    (TARGET / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
