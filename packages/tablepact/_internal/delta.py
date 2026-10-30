"""Native Delta predicate overwrite; no source preparation or retries."""

from tablepact._internal.schema import validate_delta_schema
from tablepact._internal.descriptions import apply_descriptions, description_commands
from tablepact.errors import DescriptionApplyError, WriteError, SpecError
from tablepact.contracts import WriteResult, TableSpec


def _validate_frame(df: object, spec: TableSpec) -> None:
    if getattr(df, "isStreaming", None) is not False:
        raise WriteError("overwrite requires a batch Spark DataFrame")
    columns = getattr(df, "columns", None)
    if not isinstance(columns, (list, tuple)) or not columns:
        raise WriteError("DataFrame must expose non-empty column names")
    if any(not isinstance(c, str) or not c.strip() for c in columns):
        raise WriteError("DataFrame must expose non-empty column names")
    if len({c.casefold() for c in columns}) != len(columns):
        raise WriteError("DataFrame has duplicate or case-ambiguous column names")
    schema = getattr(df, "schema", None)
    if not callable(getattr(schema, "fieldNames", None)) or schema.fieldNames() != list(columns):
        raise WriteError("DataFrame schema must match its column names")
    for column in (*spec.partition_by, *spec.cluster_by):
        if column not in columns:
            raise WriteError(f"DataFrame is missing required column: {column}")


def _require_native_predicate(df: object) -> None:
    session = getattr(df, "sparkSession", None)
    config = getattr(session, "conf", None)
    getter = getattr(config, "get", None)
    if not callable(getter):
        raise WriteError("DataFrame must expose Spark runtime configuration")
    for key in (
        "spark.databricks.delta.replaceWhere.constraintCheck.enabled",
        "spark.databricks.delta.replaceWhere.dataColumns.enabled",
    ):
        # Native defaults are true. Do not change session settings or swallow errors
        # from an unsupported runtime or denied configuration access.
        value = getter(key, "true")
        if not isinstance(value, str) or value.strip().lower() != "true":
            raise WriteError(f"Job replace_where requires {key}=true")


def overwrite(
    df: object,
    *,
    spec: TableSpec,
    replace_where: str,
) -> WriteResult:
    """Overwrite exactly the native SQL predicate with project-prepared records.

    An empty frame can delete matching rows. No filtering, date interpretation,
    schema evolution, content checks or retries are performed.
    """
    if not isinstance(replace_where, str) or not replace_where.strip():
        raise SpecError("replace_where must be a non-empty SQL expression")
    if not isinstance(spec, TableSpec) or spec.kind != "delta":
        raise SpecError("delta.overwrite requires a delta TableSpec")
    _validate_frame(df, spec)
    _require_native_predicate(df)
    validate_delta_schema(df, spec)
    commands = description_commands(df.sparkSession, spec)

    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .option("mergeSchema", "false")
        .option("overwriteSchema", "false")
    )
    if spec.partition_by:
        writer = writer.partitionBy(*spec.partition_by)
    if spec.cluster_by:
        if not callable(getattr(writer, "clusterBy", None)):
            raise RuntimeError("The Spark runtime does not provide DataFrameWriter.clusterBy")
        writer = writer.clusterBy(*spec.cluster_by)
    writer.saveAsTable(spec.target)
    try:
        apply_descriptions(df.sparkSession, spec, commands)
    except Exception as exc:
        raise DescriptionApplyError(spec.target) from exc
    return WriteResult(target=spec.target, load="replace_where")
