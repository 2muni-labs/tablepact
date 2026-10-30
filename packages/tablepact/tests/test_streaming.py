from datetime import date
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from tablepact import streaming
from tablepact._internal import streaming as internal_streaming
from tablepact.errors import SpecError
from tablepact.contracts import TableSpec, ColumnSpec
from tablepact.spec import load_spec
from helpers import streaming_spec, spark


@pytest.fixture
def dp(monkeypatch):
    native = NS(create_streaming_table=Mock(), create_auto_cdc_from_snapshot_flow=Mock(),
                create_auto_cdc_flow=Mock(), temporary_view=Mock(return_value=Mock()),
                append_flow=Mock(return_value=Mock()))
    monkeypatch.setattr(internal_streaming, '_load_pipelines', lambda: native)
    monkeypatch.setattr(internal_streaming, '_load_spark', spark)
    return native


def invoke(kind, spec=None, source=None, **kwargs):
    spec = spec if spec is not None else streaming_spec(kind, target='c.s.t')
    source = source if source is not None else Mock()
    options = {} if kind == 'append' else {'keys': ('id',)}
    if kind == 'cdc_scd2':
        options['sequence_by'] = 'seq'
    return streaming.define(spec, source=source, **(options | kwargs))


def registered_source(dp, kind):
    if kind == 'snapshot_scd2':
        return dp.create_auto_cdc_from_snapshot_flow.call_args.kwargs['source']
    decorator = dp.append_flow if kind == 'append' else dp.temporary_view
    return decorator.return_value.call_args.args[0]


@pytest.mark.parametrize('kind', ['snapshot_scd2', 'cdc_scd2', 'append'])
def test_declarations_are_lazy_return_no_write_result_and_forward_output_metadata(dp, kind):
    source = Mock()
    spec = streaming_spec(kind, target='c.s.t', cluster_by=('id',))
    assert invoke(kind, spec, source) is None
    source.assert_not_called()
    target = dp.create_streaming_table.call_args.kwargs
    assert target['name'] == 'c.s.t'
    assert target['cluster_by'] == ['id']
    assert target['comment'] == spec.description
    assert [f.metadata['comment'] for f in target['schema'].fields] == [c.description for c in spec.columns]
    if kind != 'append':
        options = (dp.create_auto_cdc_from_snapshot_flow if kind == 'snapshot_scd2' else dp.create_auto_cdc_flow).call_args.kwargs
        assert options['stored_as_scd_type'] == 2
        assert options['keys'] == ['id']


@pytest.mark.parametrize('version', [date(2026, 1, 1), 739617, '2026-01', object()])
def test_snapshot_versions_and_payload_are_passed_unchanged_without_scans_or_drop(dp, version):
    class Frame:
        isStreaming = False
        columns = ['id', 'snapshot_date', 'op']
        def forbidden(self, *args, **kwargs):
            raise AssertionError('Framework must not transform or inspect content')
        drop = select = where = filter = collect = count = isEmpty = forbidden
    frame = Frame()
    result = (frame, version)
    latest = object()
    source = Mock(return_value=result)
    invoke('snapshot_scd2', source=source)
    callback = registered_source(dp, 'snapshot_scd2')
    assert callback(latest) is result
    source.assert_called_once_with(latest)


def test_none_is_forwarded_for_initial_and_subsequent_snapshot_requests(dp):
    source = Mock(return_value=None)
    invoke('snapshot_scd2', source=source)
    callback = registered_source(dp, 'snapshot_scd2')
    assert callback(None) is None
    assert callback(123) is None
    assert [c.args for c in source.call_args_list] == [(None,), (123,)]


@pytest.mark.parametrize('result', [[], (object(),), (object(), 1, 2), 'wrong', 1])
def test_snapshot_return_shape_is_checked_at_evaluation(dp, result):
    invoke('snapshot_scd2', source=lambda _: result)
    with pytest.raises(SpecError, match='Snapshot source'):
        registered_source(dp, 'snapshot_scd2')(None)


@pytest.mark.parametrize('kind,streaming', [('snapshot_scd2', True), ('snapshot_scd2', None), ('append', False), ('cdc_scd2', False), ('append', 'true')])
def test_batch_streaming_contract_is_checked_only_at_evaluation(dp, kind, streaming):
    source = Mock(return_value=(NS(isStreaming=streaming), 1) if kind == 'snapshot_scd2' else NS(isStreaming=streaming))
    invoke(kind, source=source)
    source.assert_not_called()
    with pytest.raises(SpecError, match='Spark DataFrame'):
        callback = registered_source(dp, kind)
        callback(None) if kind == 'snapshot_scd2' else callback()


@pytest.mark.parametrize('kind', ['append', 'cdc_scd2'])
def test_streaming_frame_identity_is_preserved(dp, kind):
    frame = NS(isStreaming=True)
    invoke(kind, source=lambda: frame)
    assert registered_source(dp, kind)() is frame


class Column:
    def __bool__(self):
        raise AssertionError('Spark expressions cannot be evaluated as booleans')


def test_cdc_expressions_native_options_and_null_policy_are_not_reinterpreted(dp):
    seq, delete, key, history = (Column() for _ in range(4))
    invoke('cdc_scd2', keys=[key], sequence_by=seq, apply_as_deletes=delete,
           except_column_list=['op', 'seq'], track_history_column_list=[history], )
    options = dp.create_auto_cdc_flow.call_args.kwargs
    assert options['sequence_by'] is seq
    assert options['apply_as_deletes'] is delete
    assert options['keys'][0] is key
    assert options['track_history_column_list'][0] is history
    assert options['except_column_list'] == ['op', 'seq']
    assert options['ignore_null_updates'] is False


def test_cdc_strings_are_native_expressions_and_defaults_are_explicit(dp):
    invoke('cdc_scd2', sequence_by='sequence_column', apply_as_deletes="op = 'D'")
    opts = dp.create_auto_cdc_flow.call_args.kwargs
    assert opts['sequence_by'] == 'sequence_column'
    assert opts['apply_as_deletes'] == "op = 'D'"
    assert opts['ignore_null_updates'] is False
    assert 'except_column_list' not in opts
    assert 'track_history_column_list' not in opts


@pytest.mark.parametrize('kind', ['snapshot_scd2', 'cdc_scd2'])
def test_history_is_native_output_option_with_no_project_metadata_special_case(dp, kind):
    invoke(kind, track_history_column_list=['snapshot_date'])
    native = dp.create_auto_cdc_from_snapshot_flow if kind == 'snapshot_scd2' else dp.create_auto_cdc_flow
    assert native.call_args.kwargs['track_history_column_list'] == ['snapshot_date']


@pytest.mark.parametrize('kind,capability', [
    ('snapshot_scd2', 'create_auto_cdc_from_snapshot_flow'), ('cdc_scd2', 'create_auto_cdc_flow'),
    ('cdc_scd2', 'temporary_view'), ('append', 'append_flow'), ('append', 'create_streaming_table'),
])
def test_missing_runtime_capability_is_checked_before_any_declarations(dp, kind, capability):
    setattr(dp, capability, None)
    with pytest.raises(RuntimeError, match=capability):
        invoke(kind)
    if callable(dp.create_streaming_table):
        dp.create_streaming_table.assert_not_called()
    if callable(dp.temporary_view):
        dp.temporary_view.assert_not_called()


@pytest.mark.parametrize('kind', ['snapshot_scd2', 'cdc_scd2', 'append'])
def test_native_declaration_and_callback_exceptions_preserve_identity_without_retry(dp, kind):
    error = RuntimeError('native failure')
    source = Mock(side_effect=error)
    invoke(kind, source=source)
    with pytest.raises(RuntimeError) as caught:
        callback = registered_source(dp, kind)
        callback(None) if kind == 'snapshot_scd2' else callback()
    assert caught.value is error
    assert source.call_count == 1
    dp.create_streaming_table.reset_mock()
    dp.create_streaming_table.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        invoke(kind)
    assert caught.value is error
    dp.create_streaming_table.assert_called_once()


@pytest.mark.parametrize('kind', ['snapshot_scd2', 'cdc_scd2', 'append'])
def test_source_requires_sync_callable_before_runtime_declarations(dp, kind):
    async def async_source(): pass
    async def async_gen(): yield 1
    class AsyncCallable:
        async def __call__(self): pass
    for source in (123, async_source, async_gen, AsyncCallable()):
        with pytest.raises(SpecError, match='synchronous callable'):
            invoke(kind, source=source)
    dp.create_streaming_table.assert_not_called()


@pytest.mark.parametrize('kind', ['snapshot_scd2', 'cdc_scd2'])
@pytest.mark.parametrize('keys', [[], (), 'id', None, [''], [None]])
def test_keys_have_explicit_shape_and_are_required(dp, kind, keys):
    with pytest.raises(SpecError, match='keys'):
        invoke(kind, keys=keys)
    dp.create_streaming_table.assert_not_called()


@pytest.mark.parametrize('extra', [{'sequence_by': None}, {'sequence_by': ' '}])
def test_cdc_invalid_required_options_fail_before_declarations(dp, extra):
    with pytest.raises(SpecError):
        invoke('cdc_scd2', **extra)
    dp.create_streaming_table.assert_not_called()


@pytest.mark.parametrize('kind', ['snapshot_scd2', 'cdc_scd2', 'append'])
@pytest.mark.parametrize('extra', [{'parameters': None}, {'allow_missing': True}, {'once': True}])
def test_removed_or_unsupported_options_are_not_silently_ignored(dp, kind, extra):
    with pytest.raises(TypeError):
        invoke(kind, **extra)
    dp.create_streaming_table.assert_not_called()


def test_existing_flow_source_names_are_stable_and_snapshot_has_no_name_option(dp):
    from hashlib import sha256
    digest = sha256(b'c.s.t').hexdigest()
    invoke('cdc_scd2')
    options = dp.create_auto_cdc_flow.call_args.kwargs
    assert options['source'] == '__tablepact_source_' + digest
    assert options['name'] == '__tablepact_flow_' + digest
    invoke('append')
    assert dp.append_flow.call_args.kwargs['name'] == options['name']
    invoke('snapshot_scd2')
    assert 'name' not in dp.create_auto_cdc_from_snapshot_flow.call_args.kwargs


@pytest.mark.parametrize('kind', ['snapshot_scd2', 'cdc_scd2', 'append'])
def test_streaming_requires_parsed_spec_not_document_or_yaml(dp, kind):
    with pytest.raises(SpecError, match='streaming'):
        invoke(kind, spec='spec: {}')
    dp.create_streaming_table.assert_not_called()


def test_static_flow_is_sufficient_and_duplicate_runtime_fields_fail_before_declaration(dp):
    from tablepact.contracts import CDCFlowSpec
    spec=streaming_spec('cdc_scd2',flow=CDCFlowSpec(2,keys=['id'],sequence_by='input_seq',apply_as_deletes="op='D'"))
    source=Mock()
    streaming.define(spec,source=source)
    source.assert_not_called()
    assert dp.create_auto_cdc_flow.call_args.kwargs['sequence_by']=='input_seq'
    dp.create_streaming_table.reset_mock()
    for extra in ({'keys':['id']},{'sequence_by':Column()},{'apply_as_deletes':Column()}):
        with pytest.raises(SpecError,match='both spec and call'):
            streaming.define(spec,source=source,**extra)
    dp.create_streaming_table.assert_not_called()


@pytest.mark.parametrize('kind', ['snapshot_scd2','cdc_scd2'])
def test_required_flow_settings_are_checked_after_runtime_bindings(dp, kind):
    with pytest.raises(SpecError,match='keys are required'):
        streaming.define(streaming_spec(kind),source=Mock())
    if kind=='cdc_scd2':
        with pytest.raises(SpecError,match='sequence_by is required'):
            streaming.define(streaming_spec(kind),source=Mock(),keys=['id'])
    dp.create_streaming_table.assert_not_called()


def test_existing_pipeline_target_schema_mismatch_prevents_all_native_declarations(dp,monkeypatch):
    from helpers import Schema
    session=spark(); session.catalog.tableExists.return_value=True
    session.table.return_value=NS(schema=Schema([('id','string')]))
    monkeypatch.setattr(internal_streaming,'_load_spark',lambda:session)
    from tablepact.errors import WriteError
    with pytest.raises(WriteError,match='Existing target'):
        invoke('cdc_scd2')
    dp.create_streaming_table.assert_not_called()
    dp.temporary_view.assert_not_called()


def test_false_null_update_policy_is_a_spec_option_and_true_is_preserved(dp):
    from tablepact.contracts import CDCFlowSpec
    spec=streaming_spec('cdc_scd2',flow=CDCFlowSpec(2,keys=['id'],sequence_by='seq',ignore_null_updates=True))
    streaming.define(spec,source=Mock())
    assert dp.create_auto_cdc_flow.call_args.kwargs['ignore_null_updates'] is True


def test_append_rejects_scd2_options_before_declaration(dp):
    with pytest.raises(SpecError,match='not supported'):
        streaming.define(streaming_spec('append'),source=Mock(),keys=['id'])
    dp.create_streaming_table.assert_not_called()


@pytest.mark.parametrize('kwargs',[dict(keys=['__START_AT']),dict(keys=['id'],except_column_list=['ID'])])
def test_runtime_keys_and_exclusions_cannot_remove_output_identity(dp,kwargs):
    with pytest.raises(SpecError):
        invoke('cdc_scd2',**kwargs)
    dp.create_streaming_table.assert_not_called()


@pytest.mark.parametrize('field',['keys','except_column_list','track_history_column_list'])
@pytest.mark.parametrize('columns',[['id','id'],['id','ID']])
def test_runtime_name_lists_use_static_duplicate_and_case_rules(dp,field,columns):
    with pytest.raises(SpecError,match='duplicate column names'):
        invoke('cdc_scd2',**{field:columns})
    dp.create_streaming_table.assert_not_called()
    dp.temporary_view.assert_not_called()


def test_runtime_mixed_native_columns_are_passed_unchanged_while_names_are_checked(dp):
    key=Column()
    invoke('cdc_scd2',keys=['id',key])
    assert dp.create_auto_cdc_flow.call_args.kwargs['keys'][1] is key
    dp.create_streaming_table.reset_mock()
    with pytest.raises(SpecError,match='duplicate column names'):
        invoke('cdc_scd2',keys=['id',key,'ID'])
    dp.create_streaming_table.assert_not_called()
