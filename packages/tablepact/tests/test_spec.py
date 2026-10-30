from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest
import yaml

from tablepact.errors import SpecError
from tablepact.spec import load_spec
from tablepact.contracts import AppendFlowSpec, CDCFlowSpec, ColumnSpec, SnapshotFlowSpec, SpecDocument, TableSpec
from helpers import table, streaming_spec


def document():
    return dict(spec=dict(kind='delta', target='c.s.t', description='Table description',
                         columns=[dict(name='id', type='BIGINT', description='Identifier')]))


def test_yaml_and_python_contracts_and_optional_options(tmp_path):
    raw = document()
    text = yaml.safe_dump(raw)
    path = tmp_path / 'table.yaml'; path.write_text(text)
    expected = SpecDocument(TableSpec('delta', 'c.s.t', 'Table description', (ColumnSpec('id','BIGINT','Identifier'),)))
    assert load_spec(text) == load_spec(path) == expected
    with pytest.raises(FrozenInstanceError):
        expected.spec.target = 'x'


def test_options_are_opaque_and_never_interpreted_or_merged():
    raw = document()
    raw['options'] = {'transform_sql': 'SELECT :x FROM source', 'custom': {'unexpected': [1, True]}, 'kind': 'unrelated'}
    loaded = load_spec(yaml.safe_dump(raw))
    assert loaded.options == raw['options']
    assert loaded.spec.kind == 'delta'


@pytest.mark.parametrize('bad', [None, [], 'sql'])
def test_options_must_be_a_mapping(bad):
    raw = document(); raw['options'] = bad
    with pytest.raises(SpecError): load_spec(yaml.safe_dump(raw))


@pytest.mark.parametrize('field', ['kind','target','description','columns'])
def test_required_spec_fields(field):
    raw = document(); del raw['spec'][field]
    with pytest.raises(SpecError): load_spec(yaml.safe_dump(raw))


@pytest.mark.parametrize('bad', ['', ' ', None, 1, []])
@pytest.mark.parametrize('field', ['name','type','description'])
def test_column_fields_require_nonempty_text(field, bad):
    args = dict(name='id', type='BIGINT', description='Identifier'); args[field] = bad
    with pytest.raises(SpecError): ColumnSpec(**args)


@pytest.mark.parametrize('text', ['', '[]', 'target: t', 'spec: {}\nextra: true', 'spec: {}\nspec: {}', 'spec: [', 'spec: {kind: delta, kind: streaming}'])
def test_malformed_duplicate_or_old_root_is_rejected(text):
    with pytest.raises(SpecError): load_spec(text)


@pytest.mark.parametrize('field', ['output_schema', 'backend', 'load', 'unknown'])
def test_unknown_spec_fields_are_rejected(field):
    raw = document(); raw['spec'][field] = 'x'
    with pytest.raises(SpecError): load_spec(yaml.safe_dump(raw))


@pytest.mark.parametrize('columns', [[], [ColumnSpec('id','INT','a'),ColumnSpec('ID','INT','b')], [{}]])
def test_output_columns_must_be_nonempty_unique_column_specs(columns):
    with pytest.raises(SpecError): table(columns=columns)


@pytest.mark.parametrize('layout', ['partition_by','cluster_by'])
@pytest.mark.parametrize('value', ['id', None, ['id','ID'], ['missing']])
def test_invalid_layout(layout, value):
    with pytest.raises(SpecError): table(**{layout: value})


def test_layouts_are_exclusive_and_flow_is_kind_dependent():
    for args in (dict(partition_by=['id'],cluster_by=['id']), dict(flow=AppendFlowSpec()), dict(kind='streaming'), dict(kind='unknown')):
        with pytest.raises(SpecError): table(**args)


@pytest.mark.parametrize('kind,flow', [('auto_cdc_from_snapshot',SnapshotFlowSpec(2,keys=['id'])),('auto_cdc',CDCFlowSpec(2,keys=['id'],sequence_by='seq')),('append',AppendFlowSpec())])
def test_typed_flow_yaml_matches_python(kind, flow):
    spec = streaming_spec('append' if kind=='append' else 'snapshot_scd2', flow=flow)
    from dataclasses import asdict
    raw = {'spec': asdict(spec)}
    loaded = load_spec(yaml.safe_dump(raw))
    assert loaded.spec == spec


@pytest.mark.parametrize('value', [1, '2', True, None])
def test_only_scd2_is_supported(value):
    with pytest.raises(SpecError): SnapshotFlowSpec(value)


def test_scd2_requires_system_columns_but_source_only_sequence_is_valid():
    with pytest.raises(SpecError, match='system|__START_AT'):
        table(kind='streaming',flow=SnapshotFlowSpec(2, keys=['id']), columns=[ColumnSpec('id','BIGINT','Identifier')])
    streaming_spec('cdc_scd2', flow=CDCFlowSpec(2,keys=['id'],sequence_by='input_only',except_column_list=['input_only','op']))


@pytest.mark.parametrize('kwargs', [dict(keys=['missing']),dict(track_history_column_list=['missing']),dict(track_history_column_list=['__START_AT']),dict(keys=['id'],except_column_list=['id'])])
def test_static_references_are_checked(kwargs):
    with pytest.raises(SpecError): streaming_spec('cdc_scd2',flow=CDCFlowSpec(2,**kwargs))


@pytest.mark.parametrize('flow', [None, {}, {'type':'unknown'},{'type':'append','keys':['id']},{'type':'auto_cdc','stored_as_scd_type':1},{'type':'auto_cdc_from_snapshot'}])
def test_invalid_flow_yaml_is_rejected(flow):
    raw=document(); raw['spec'].update(kind='streaming',flow=flow)
    with pytest.raises(SpecError): load_spec(yaml.safe_dump(raw))


@pytest.mark.parametrize('value', [None,1,{},[]])
def test_loader_requires_yaml_text_or_explicit_path(value):
    with pytest.raises(SpecError): load_spec(value)


def test_required_column_description_and_unknown_column_fields_are_checked():
    for field in ['description','extra']:
        raw=document()
        if field=='description': del raw['spec']['columns'][0][field]
        else: raw['spec']['columns'][0][field]='x'
        with pytest.raises(SpecError): load_spec(yaml.safe_dump(raw))


def test_options_internal_mapping_keys_are_project_owned():
    raw=document(); raw['options']={'codes':{1:'one',2:'two'}}
    assert load_spec(yaml.safe_dump(raw)).options==raw['options']
    raw['spec'][1]='not a spec field'
    with pytest.raises(SpecError,match='field names'):
        load_spec(yaml.safe_dump(raw,sort_keys=False))


def test_delta_flow_null_matches_absence_and_python_document_roundtrip():
    from dataclasses import asdict
    original=SpecDocument(table(),{'transform_sql':'SELECT * FROM source'})
    text=yaml.safe_dump(asdict(original))
    assert load_spec(text)==original
    raw=document()
    omitted=load_spec(yaml.safe_dump(raw))
    raw['spec']['flow']=None
    assert load_spec(yaml.safe_dump(raw))==omitted


def test_streaming_flow_null_is_still_rejected():
    raw=document(); raw['spec'].update(kind='streaming',flow=None)
    with pytest.raises(SpecError,match='requires a supported flow'):
        load_spec(yaml.safe_dump(raw))


@pytest.mark.parametrize('kind',['delta_table','streaming_table','view'])
def test_old_kind_aliases_and_unimplemented_view_are_rejected(kind):
    raw=document(); raw['spec']['kind']=kind
    with pytest.raises(SpecError,match='kind must be delta or streaming'):
        load_spec(yaml.safe_dump(raw))
    with pytest.raises(SpecError,match='kind must be delta or streaming'):
        table(kind=kind)
