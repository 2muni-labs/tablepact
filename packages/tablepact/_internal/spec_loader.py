"""A small YAML loader; TableSpec constructors own the shared validation path."""

from dataclasses import fields
from pathlib import Path

import yaml

from tablepact.errors import SpecError
from tablepact.contracts import (
    AppendFlowSpec, CDCFlowSpec, ColumnSpec, SnapshotFlowSpec, SpecDocument, TableSpec,
)


class _UniqueLoader(yaml.SafeLoader):
    """Reject duplicate YAML keys instead of silently selecting a policy."""

    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError as exc:
                raise SpecError("YAML mapping keys must be hashable") from exc
            if duplicate:
                raise SpecError("Duplicate YAML mapping key")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _check_fields(data: object, model: type, required: set[str]) -> dict:
    if not isinstance(data, dict):
        raise SpecError(f"{model.__name__} must be a mapping")
    if any(not isinstance(key, str) for key in data):
        raise SpecError(f"{model.__name__} field names must be strings")
    allowed = {field.name for field in fields(model) if field.init}
    unknown = set(data) - allowed
    missing = required - set(data)
    if unknown:
        raise SpecError(f"Unknown {model.__name__} fields: {sorted(unknown)}")
    if missing:
        raise SpecError(f"Missing {model.__name__} fields: {sorted(missing)}")
    return dict(data)


def load_spec(source: str | Path) -> SpecDocument:
    if isinstance(source, Path):
        text = source.read_text(encoding="utf-8")
    elif isinstance(source, str):
        text = source
    else:
        raise SpecError("Spec source must be YAML text or an explicit Path")
    try:
        data = yaml.load(text, Loader=_UniqueLoader)
    except yaml.YAMLError as exc:
        # Do not copy parser output, which can include arbitrary source content.
        raise SpecError("Invalid Table Spec YAML") from exc
    if not isinstance(data, dict) or "spec" not in data or set(data) - {"spec", "options"}:
        raise SpecError("Document requires spec and allows only optional options")
    values = _check_fields(data["spec"], TableSpec, {"kind", "target", "description", "columns"})
    if not isinstance(values["columns"], list):
        raise SpecError("columns must be a list")
    values["columns"] = tuple(ColumnSpec(**_check_fields(c, ColumnSpec, {"name", "type", "description"}))
                              for c in values["columns"])
    if values.get("flow") is not None:
        raw = values["flow"]
        models = {"auto_cdc_from_snapshot": SnapshotFlowSpec, "auto_cdc": CDCFlowSpec, "append": AppendFlowSpec}
        if not isinstance(raw, dict) or not isinstance(raw.get("type"), str) or raw["type"] not in models:
            raise SpecError("flow.type must identify a supported native flow")
        model = models[raw["type"]]
        required = set() if model is AppendFlowSpec else {"stored_as_scd_type"}
        args = _check_fields({k: v for k, v in raw.items() if k != "type"}, model, required)
        values["flow"] = model(**args)
    return SpecDocument(TableSpec(**values), data.get("options", {}))
