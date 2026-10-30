"""Small immutable public contracts independent of Spark and Databricks."""

from dataclasses import dataclass, field
from typing import Literal

from tablepact.errors import SpecError

__all__ = ["ColumnSpec", "TableSpec",
           "SnapshotFlowSpec", "CDCFlowSpec", "AppendFlowSpec", "SpecDocument", "WriteResult"]


def _names(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        raise SpecError(f"{field} must be a list or tuple of column names")
    if any(not isinstance(x, str) or not x.strip() for x in value):
        raise SpecError(f"{field} must contain non-empty column names")
    if len({x.casefold() for x in value}) != len(value):
        raise SpecError(f"{field} contains duplicate column names")
    return tuple(value)


def _text(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise SpecError(f"{name} must be a non-empty string")


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    description: str

    def __post_init__(self):
        for name in ("name", "type", "description"):
            _text(getattr(self, name), f"column.{name}")


def _scd2(flow):
    if type(flow.stored_as_scd_type) is not int or flow.stored_as_scd_type != 2:
        raise SpecError("Only stored_as_scd_type=2 is supported")
    for name in ("keys", "track_history_column_list"):
        value = getattr(flow, name)
        if value is not None:
            value = _names(value, name)
            if name == "keys" and not value:
                raise SpecError("keys must not be empty")
            object.__setattr__(flow, name, value)


@dataclass(frozen=True)
class SnapshotFlowSpec:
    stored_as_scd_type: int
    keys: tuple[str, ...] | None = None
    track_history_column_list: tuple[str, ...] | None = None
    type: Literal["auto_cdc_from_snapshot"] = field(default="auto_cdc_from_snapshot", init=False)

    def __post_init__(self):
        _scd2(self)


@dataclass(frozen=True)
class CDCFlowSpec:
    stored_as_scd_type: int
    keys: tuple[str, ...] | None = None
    sequence_by: str | None = None
    apply_as_deletes: str | None = None
    except_column_list: tuple[str, ...] | None = None
    track_history_column_list: tuple[str, ...] | None = None
    ignore_null_updates: bool = False
    type: Literal["auto_cdc"] = field(default="auto_cdc", init=False)

    def __post_init__(self):
        _scd2(self)
        for name in ("sequence_by", "apply_as_deletes"):
            value = getattr(self, name)
            if value is not None:
                _text(value, name)
        if self.except_column_list is not None:
            object.__setattr__(self, "except_column_list", _names(self.except_column_list, "except_column_list"))
        if type(self.ignore_null_updates) is not bool:
            raise SpecError("ignore_null_updates must be a boolean")


@dataclass(frozen=True)
class AppendFlowSpec:
    type: Literal["append"] = field(default="append", init=False)


@dataclass(frozen=True)
class TableSpec:
    """Output contract; source expressions and project options stay separate."""

    kind: Literal["delta", "streaming"]
    target: str
    description: str
    columns: tuple[ColumnSpec, ...]
    partition_by: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()
    flow: SnapshotFlowSpec | CDCFlowSpec | AppendFlowSpec | None = None

    def __post_init__(self):
        if self.kind not in ("delta", "streaming"):
            raise SpecError("kind must be delta or streaming")
        _text(self.target, "target")
        _text(self.description, "description")
        if not isinstance(self.columns, (tuple, list)) or not self.columns or any(
            not isinstance(c, ColumnSpec) for c in self.columns
        ):
            raise SpecError("columns must be a non-empty list or tuple of ColumnSpec")
        object.__setattr__(self, "columns", tuple(self.columns))
        _names([c.name for c in self.columns], "columns")
        for name in ("partition_by", "cluster_by"):
            object.__setattr__(self, name, _names(getattr(self, name), name))
        if self.partition_by and self.cluster_by:
            raise SpecError("partition_by and cluster_by are mutually exclusive")
        if self.kind == "delta" and self.flow is not None:
            raise SpecError("delta must not define flow")
        if self.kind == "streaming" and not isinstance(
            self.flow, (SnapshotFlowSpec, CDCFlowSpec, AppendFlowSpec)
        ):
            raise SpecError("streaming requires a supported flow")
        names = {c.name for c in self.columns}
        for name in ("partition_by", "cluster_by"):
            self._references(getattr(self, name), names, name)
        if isinstance(self.flow, (SnapshotFlowSpec, CDCFlowSpec)):
            if not {"__START_AT", "__END_AT"}.issubset(names):
                raise SpecError("SCD2 columns must include __START_AT and __END_AT")
            self._references(self.flow.keys or (), names, "keys")
            if {"__START_AT", "__END_AT"}.intersection(self.flow.keys or ()):
                raise SpecError("Keys cannot include SCD2 system columns")
            history = self.flow.track_history_column_list or ()
            self._references(history, names, "track_history_column_list")
            if {"__START_AT", "__END_AT"}.intersection(history):
                raise SpecError("History columns cannot include SCD2 system columns")
            if isinstance(self.flow, CDCFlowSpec):
                excluded = {c.casefold() for c in self.flow.except_column_list or ()}
                protected = (*self.partition_by, *self.cluster_by, *(self.flow.keys or ()), *history)
                if excluded.intersection(c.casefold() for c in protected):
                    raise SpecError("CDC exclusions cannot remove keys, layout or history columns")

    @staticmethod
    def _references(columns, names, label):
        if any(c not in names for c in columns):
            raise SpecError(f"{label} must reference output columns")


@dataclass(frozen=True)
class SpecDocument:
    spec: TableSpec
    options: dict = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.spec, TableSpec):
            raise SpecError("spec must be a TableSpec")
        if not isinstance(self.options, dict):
            raise SpecError("options must be a mapping")


@dataclass(frozen=True)
class WriteResult:
    """A native Job write returned; no inferred commit version or row metrics."""

    target: str
    load: Literal["replace_where"]
