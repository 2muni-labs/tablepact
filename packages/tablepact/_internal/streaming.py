"""Thin, lazy Lakeflow declarations. Projects own source and business policies."""

from collections.abc import Callable
from hashlib import sha256
from importlib import import_module
from inspect import isasyncgenfunction, iscoroutinefunction

from tablepact.errors import SpecError
from tablepact._internal.schema import declared_schema, validate_target_schema
from tablepact.contracts import TableSpec, _names


def _load_pipelines():
    try:
        return import_module("pyspark.pipelines")
    except ModuleNotFoundError as exc:
        if exc.name not in ("pyspark", "pyspark.pipelines"):
            raise
        raise RuntimeError("Pipeline declarations require a Databricks Lakeflow runtime") from exc


def _require_capabilities(dp, *names):
    for name in names:
        if not callable(getattr(dp, name, None)):
            raise RuntimeError(f"The Lakeflow runtime does not provide {name}")


def _internal_name(kind, target):
    # Preserve the existing deterministic names: flow names carry native state.
    return "__tablepact_" + kind + "_" + sha256(target.encode("utf-8")).hexdigest()


def _load_spark():
    sql = import_module("pyspark.sql")
    spark = sql.SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Pipeline declarations require an active Spark session")
    return spark


def _target_options(spec, schema):
    options = {"name": spec.target, "comment": spec.description, "schema": schema}
    if spec.partition_by:
        options["partition_cols"] = list(spec.partition_by)
    if spec.cluster_by:
        options["cluster_by"] = list(spec.cluster_by)
    return options


def _prepare(spec, source):
    if not isinstance(spec, TableSpec) or spec.kind != "streaming":
        raise SpecError("streaming.define requires a streaming TableSpec")
    call = getattr(source, "__call__", None)
    if not callable(source) or any(
        check(value) for check in (iscoroutinefunction, isasyncgenfunction)
        for value in (source, call)
    ):
        raise SpecError("source must be a synchronous callable")


def _columns(value, name, *, required=False):
    # Native supports column names and Spark Column expressions. Never evaluate
    # a Column's truth value or build/parse project expressions here.
    if not isinstance(value, (tuple, list)) or (required and not value):
        raise SpecError(f"{name} must be a {'non-empty ' if required else ''}list or tuple")
    for column in value:
        if column is None or (isinstance(column, str) and not column.strip()):
            raise SpecError(f"{name} contains an empty column")
    _names([column for column in value if isinstance(column, str)], name)
    return list(value)


def _frame(frame, *, streaming):
    if getattr(frame, "isStreaming", None) is not streaming:
        kind = "streaming" if streaming else "batch"
        raise SpecError(f"source must return a {kind} Spark DataFrame")
    return frame


def _snapshot_source(source):
    def next_snapshot(latest_version):
        result = source(latest_version)
        if result is None:
            return None
        if not isinstance(result, tuple) or len(result) != 2:
            raise SpecError("Snapshot source must return (batch DataFrame, native version) or None")
        _frame(result[0], streaming=False)
        return result
    return next_snapshot


def _streaming_source(source):
    def read_stream():
        return _frame(source(), streaming=True)
    return read_stream


_UNSET = object()


def _resolve(spec, bindings):
    flow = spec.flow
    allowed = {
        "auto_cdc_from_snapshot": {"keys", "track_history_column_list"},
        "auto_cdc": {"keys", "sequence_by", "apply_as_deletes", "except_column_list", "track_history_column_list"},
        "append": set(),
    }[flow.type]
    result = {}
    for name, value in bindings.items():
        static = getattr(flow, name, None)
        if value is not _UNSET and name not in allowed:
            raise SpecError(f"{name} is not supported for {flow.type}")
        if value is not _UNSET and static is not None:
            raise SpecError(f"{name} is defined in both spec and call arguments")
        selected = static if value is _UNSET else value
        if selected is None:
            if value is not _UNSET:
                raise SpecError(f"{name} must not be None")
            continue
        if name in ("keys", "except_column_list", "track_history_column_list"):
            selected = _columns(selected, name, required=name == "keys")
        elif isinstance(selected, str) and not selected.strip():
            raise SpecError(f"{name} must not be empty")
        result[name] = selected
    if flow.type != "append" and "keys" not in result:
        raise SpecError("keys are required in spec or call arguments")
    if flow.type == "auto_cdc" and "sequence_by" not in result:
        raise SpecError("sequence_by is required in spec or call arguments")
    # Validate known names only: opaque Spark Column expressions remain native.
    names = {c.name for c in spec.columns}
    for field in ("keys", "track_history_column_list"):
        for value in result.get(field, ()):
            if isinstance(value, str) and value not in names:
                raise SpecError(f"{field} must reference output columns")
            if isinstance(value, str) and value in ("__START_AT", "__END_AT"):
                raise SpecError("Keys and history columns cannot include SCD2 system columns")
    protected = {*spec.partition_by, *spec.cluster_by}
    for field in ("keys", "track_history_column_list"):
        protected.update(v for v in result.get(field, ()) if isinstance(v, str))
    if {v.casefold() for v in protected}.intersection(v.casefold() for v in result.get("except_column_list", ()) if isinstance(v, str)):
        raise SpecError("CDC exclusions cannot remove keys, layout or history columns")
    return result


def define(
    spec: TableSpec, *, source: Callable, keys=_UNSET, sequence_by=_UNSET,
    apply_as_deletes=_UNSET, except_column_list=_UNSET, track_history_column_list=_UNSET,
) -> None:
    """Declare the spec's flow, binding optional project-prepared native expressions.

    Duplicate static/runtime fields are rejected before any native declarations.
    Sources are evaluated only by Lakeflow; no business transformations are added.
    """
    _prepare(spec, source)
    options = _resolve(spec, dict(keys=keys, sequence_by=sequence_by,
                                 apply_as_deletes=apply_as_deletes,
                                 except_column_list=except_column_list,
                                 track_history_column_list=track_history_column_list))
    dp = _load_pipelines()
    native = {"auto_cdc_from_snapshot": "create_auto_cdc_from_snapshot_flow",
              "auto_cdc": "create_auto_cdc_flow", "append": "append_flow"}[spec.flow.type]
    capabilities = ["create_streaming_table", native]
    if spec.flow.type == "auto_cdc":
        capabilities.append("temporary_view")
    _require_capabilities(dp, *capabilities)
    spark = _load_spark()
    schema = declared_schema(spark, spec)
    validate_target_schema(spark, spec, schema)
    options["target"] = spec.target
    if spec.flow.type == "auto_cdc_from_snapshot":
        options.update(source=_snapshot_source(source), stored_as_scd_type=2)
        dp.create_streaming_table(**_target_options(spec, schema))
        dp.create_auto_cdc_from_snapshot_flow(**options)
    elif spec.flow.type == "auto_cdc":
        options.update(source=_internal_name("source", spec.target), stored_as_scd_type=2,
                       ignore_null_updates=spec.flow.ignore_null_updates,
                       name=_internal_name("flow", spec.target))
        dp.temporary_view(name=options["source"])(_streaming_source(source))
        dp.create_streaming_table(**_target_options(spec, schema))
        dp.create_auto_cdc_flow(**options)
    else:
        dp.create_streaming_table(**_target_options(spec, schema))
        dp.append_flow(target=spec.target, name=_internal_name("flow", spec.target))(
            _streaming_source(source))
