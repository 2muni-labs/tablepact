from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tablepact.delta import overwrite
from tablepact.errors import WriteError, SpecError
from tablepact.contracts import WriteResult, TableSpec
from helpers import Schema as NativeSchema, spark, table

CHECK = "spark.databricks.delta.replaceWhere.constraintCheck.enabled"
DATA_COLUMNS = "spark.databricks.delta.replaceWhere.dataColumns.enabled"


class Schema:
    def __init__(self, columns, window_column="processing_date", window_type="date"):
        self.columns = list(columns)
        self.window_column = window_column
        self.window_type = window_type

    def fieldNames(self):
        return self.columns

    def __getitem__(self, column):
        assert column == self.window_column
        return SimpleNamespace(dataType=SimpleNamespace(typeName=lambda: self.window_type))


class Writer:
    def __init__(self):
        self.calls = []

    def format(self, source):
        self.calls.append(("format", source)); return self

    def mode(self, mode):
        self.calls.append(("mode", mode)); return self

    def option(self, key, value):
        self.calls.append(("option", key, value)); return self

    def partitionBy(self, *cols):
        self.calls.append(("partition", cols)); return self

    def clusterBy(self, *cols):
        self.calls.append(("cluster", cols)); return self

    def saveAsTable(self, name):
        self.calls.append(("save", name))


class BatchFrame:
    isStreaming = False

    def __init__(self, columns=("id", "processing_date"), *, window_column="processing_date", window_type="date"):
        self.columns = list(columns)
        self.schema = NativeSchema([(name, "bigint" if name == "id" else window_type) for name in self.columns])
        self.writer = Writer()
        self.sparkSession = spark()
        self.sparkSession.table.return_value = SimpleNamespace(schema=self.schema)
        self.config = self.sparkSession.config
        self.get_config = self.sparkSession.conf.get
        self.write_accesses = 0

    @property
    def write(self):
        self.write_accesses += 1
        return self.writer

    def scan(self, *args, **kwargs):
        raise AssertionError("No implicit scans, filtering, snapshot inventory, or history reads")

    count = collect = isEmpty = filter = where = select = distinct = alias = scan


def spec(**kwargs):
    return table(**kwargs)


@pytest.mark.parametrize("layout,native", [("partition_by", "partition"), ("cluster_by", "cluster")])
def test_native_layout_is_forwarded_in_order_without_ddl(layout, native):
    frame = BatchFrame()
    overwrite(frame, spec=spec(**{layout: ("processing_date", "id")}), replace_where="id > 0")
    assert frame.writer.calls[-2:] == [(native, ("processing_date", "id")), ("save", "c.s.output")]


def test_missing_cluster_api_prevents_write():
    frame = BatchFrame()
    frame.writer.clusterBy = None
    with pytest.raises(RuntimeError, match="clusterBy"):
        overwrite(frame, spec=spec(cluster_by=("id",)), replace_where="id > 0")
    assert not any(c[0] == "save" for c in frame.writer.calls)


def test_empty_records_are_not_skipped_or_checked():
    frame = BatchFrame()
    frame.rows = []  # Project has intentionally prepared an empty result.
    overwrite(frame, spec=spec(), replace_where="id > 0")
    assert frame.writer.calls[-1] == ("save", "c.s.output")


@pytest.mark.parametrize("key", [CHECK, DATA_COLUMNS])
@pytest.mark.parametrize("value", ["false", "FALSE", "0", "", None, False, 0])
def test_disabled_or_invalid_predicate_settings_fail_before_writer_access(key, value):
    frame = BatchFrame()
    frame.config[key] = value
    with pytest.raises(WriteError, match="requires .*true"):
        overwrite(frame, spec=spec(), replace_where="id > 0")
    assert frame.write_accesses == 0
    assert frame.config[key] == value  # No config mutation.


def test_enabled_settings_and_ambient_dynamic_overwrite_do_not_add_conflicting_option():
    frame = BatchFrame()
    frame.config.update({CHECK: "TRUE", DATA_COLUMNS: "true", "spark.sql.sources.partitionOverwriteMode": "dynamic"})
    overwrite(frame, spec=spec(), replace_where="id > 0")
    assert not any(c[:2] == ("option", "partitionOverwriteMode") for c in frame.writer.calls)
    assert frame.config["spark.sql.sources.partitionOverwriteMode"] == "dynamic"


def test_native_config_getter_failure_propagates_without_mutation_or_write():
    frame = BatchFrame()
    error = RuntimeError("native configuration not available")
    frame.get_config.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        overwrite(frame, spec=spec(), replace_where="id > 0")
    assert caught.value is error
    assert frame.write_accesses == 0


@pytest.mark.parametrize("session", [None, object(), SimpleNamespace(conf=object())])
def test_unavailable_runtime_config_is_rejected_before_write(session):
    frame = BatchFrame()
    frame.sparkSession = session
    with pytest.raises(WriteError, match="configuration"):
        overwrite(frame, spec=spec(), replace_where="id > 0")
    assert frame.write_accesses == 0


@pytest.mark.parametrize("value", [True, None, "false", 0])
def test_non_batch_input_is_rejected(value):
    frame = BatchFrame()
    frame.isStreaming = value
    with pytest.raises(WriteError, match="batch"):
        overwrite(frame, spec=spec(), replace_where="id > 0")
    assert frame.write_accesses == 0


@pytest.mark.parametrize("columns", [[], [""], [None], [1], ["id", "ID"], ["id", "id"]])
def test_malformed_columns_fail_before_write(columns):
    frame = BatchFrame(columns)
    with pytest.raises(WriteError, match="column"):
        overwrite(frame, spec=spec(), replace_where="id > 0")
    assert frame.write_accesses == 0


@pytest.mark.parametrize("schema", [None, object(), SimpleNamespace(fieldNames=lambda: ["different"])])
def test_schema_metadata_must_match_columns(schema):
    frame = BatchFrame()
    frame.schema = schema
    with pytest.raises(WriteError, match="schema"):
        overwrite(frame, spec=spec(), replace_where="id > 0")
    assert frame.write_accesses == 0


def test_invalid_yaml_does_not_access_writer():
    frame = BatchFrame()
    with pytest.raises(SpecError):
        overwrite(frame, spec="target: [not, a, table]", replace_where="id > 0")
    assert frame.write_accesses == 0


def test_native_write_failure_preserves_original_without_result_or_retry():
    frame = BatchFrame()
    error = RuntimeError("native replaceWhere constraint violation")
    frame.writer.saveAsTable = Mock(side_effect=error)
    with pytest.raises(RuntimeError) as caught:
        overwrite(frame, spec=spec(), replace_where="id > 0")
    assert caught.value is error
    frame.writer.saveAsTable.assert_called_once_with("c.s.output")


def test_write_result_is_minimal_immutable_and_has_no_inferred_metrics():
    result = overwrite(BatchFrame(), spec=spec(), replace_where="id > 0")
    assert vars(result) == {"target": "c.s.output", "load": "replace_where"}
    with pytest.raises(FrozenInstanceError):
        result.target = "other"


@pytest.mark.parametrize("predicate", [
    "id IN (1, 3)", "status = 'done'", "TRUE",
    "processing_date >= TIMESTAMP '2026-01-01 00:00:00'",
    " `event.date` = 'x' OR id = 8 ",
])
def test_project_predicate_is_forwarded_verbatim_without_parsing_or_type_restriction(predicate):
    frame = BatchFrame()
    result = overwrite(frame, spec=spec(), replace_where=predicate)
    assert frame.writer.calls == [
        ("format", "delta"), ("mode", "overwrite"),
        ("option", "replaceWhere", predicate),
        ("option", "mergeSchema", "false"), ("option", "overwriteSchema", "false"),
        ("save", "c.s.output"),
    ]
    assert result == WriteResult("c.s.output", "replace_where")


@pytest.mark.parametrize("predicate", [None, "", "  ", False, [], 1])
def test_invalid_predicate_fails_before_frame_access(predicate):
    with pytest.raises(SpecError, match="replace_where"):
        overwrite(object(), spec=spec(), replace_where=predicate)


def test_predicate_is_required_and_old_parameter_argument_is_rejected():
    with pytest.raises(TypeError):
        overwrite(object(), spec=spec())
    with pytest.raises(TypeError):
        overwrite(object(), spec=spec(), replace_where="TRUE", parameters=None)


@pytest.mark.parametrize("layout", ["partition_by", "cluster_by"])
def test_missing_layout_column_is_rejected(layout):
    frame = BatchFrame()
    with pytest.raises(SpecError, match="output columns"):
        overwrite(frame, spec=spec(**{layout: ("absent",)}), replace_where="TRUE")
    assert frame.write_accesses == 0


def test_writer_requires_parsed_delta_spec():
    for value in ("spec: {}", None, object()):
        frame = BatchFrame()
        with pytest.raises(SpecError, match="delta"):
            overwrite(frame, spec=value, replace_where="TRUE")
        assert frame.write_accesses == 0
