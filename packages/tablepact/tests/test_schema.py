from dataclasses import replace
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from tablepact._internal.schema import declared_schema, validate_delta_schema, validate_target_schema
from tablepact.errors import SpecError, WriteError
from tablepact.contracts import ColumnSpec
from helpers import Schema, spark, table, streaming_spec


def frame(session, columns=None):
    return NS(sparkSession=session, schema=Schema(columns or [('id','bigint'),('processing_date','date')]))


def test_schema_is_native_parsed_and_comments_are_metadata_without_data_reads():
    session = spark()
    parsed = validate_delta_schema(frame(session), table())
    session.createDataFrame.assert_called_once_with([], schema='`id` BIGINT, `processing_date` DATE')
    session.catalog.tableExists.assert_called_once_with('c.s.output')
    session.table.assert_not_called()
    assert [f.metadata for f in parsed.fields] == [{'comment':'id description'},{'comment':'processing_date description'}]


@pytest.mark.parametrize('columns', [[('id','string'),('processing_date','date')], [('processing_date','date'),('id','bigint')], [('other','bigint')]])
def test_source_schema_mismatch_prevents_target_lookup(columns):
    session = spark()
    with pytest.raises(WriteError, match='Input'):
        validate_delta_schema(frame(session, columns), table())
    session.catalog.tableExists.assert_not_called()


def test_existing_target_mismatch_fails_without_migration():
    session = spark(); session.catalog.tableExists.return_value = True
    session.table.return_value = NS(schema=Schema([('id','string')]))
    with pytest.raises(WriteError, match='Existing target'):
        validate_delta_schema(frame(session), table())
    session.sql.assert_not_called()


def test_existing_target_ignores_description_drift_but_compares_types_and_order():
    session = spark(); session.catalog.tableExists.return_value = True
    old = Schema([('id','bigint'),('processing_date','date')])
    old.fields[0].metadata['comment'] = 'old'
    session.table.return_value = NS(schema=old)
    validate_delta_schema(frame(session), table())


@pytest.mark.parametrize('dtype', ['date', NS(fields=[NS(nullable=False,dataType='string')]), NS(elementType=NS(valueType=NS(fields=[NS(nullable=False,dataType='string')])) )])
def test_top_level_and_nested_not_null_are_rejected(dtype):
    session = spark(); schema = Schema([('id',dtype)])
    schema.fields[0].nullable = dtype != 'date'
    session.createDataFrame.side_effect = None
    session.createDataFrame.return_value = NS(schema=schema)
    spec = table(columns=[ColumnSpec('id','DATE','Identifier')])
    with pytest.raises(SpecError, match='NOT NULL'):
        declared_schema(session, spec)


def test_native_parser_and_catalog_failures_preserve_identity():
    session = spark(); error = RuntimeError('native parser')
    session.createDataFrame.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        validate_delta_schema(frame(session), table())
    assert caught.value is error


def test_type_field_cannot_add_output_fields():
    session = spark(); session.createDataFrame.side_effect = None
    session.createDataFrame.return_value = NS(schema=Schema([('id','bigint'),('extra','string')]))
    with pytest.raises(SpecError, match='exactly one'):
        declared_schema(session, table(columns=[ColumnSpec('id','BIGINT, extra STRING','Identifier')]))
    session.sql.assert_not_called()


def test_nested_types_and_existing_metadata_survive_comment_attachment():
    session = spark(); session.createDataFrame.side_effect = None
    dtype = {'type':'struct','fields':[{'name':'nested','type':'string','nullable':True,'metadata':{}}]}
    parsed = Schema([('id',dtype)]); parsed.fields[0].metadata['custom']='keep'
    session.createDataFrame.return_value=NS(schema=parsed)
    result=declared_schema(session, table(columns=[ColumnSpec('id','STRUCT<nested: STRING>','Nested description')]))
    assert result.fields[0].dataType == dtype
    assert result.fields[0].metadata == {'custom':'keep','comment':'Nested description'}


def test_scd2_system_column_types_are_explicit_and_must_match():
    spec = streaming_spec('snapshot_scd2')
    columns = [replace(c,type='STRING') if c.name=='__END_AT' else c for c in spec.columns]
    with pytest.raises(SpecError, match='system column types'):
        declared_schema(spark(), replace(spec,columns=columns))


def nested_type(comment):
    return {'type':'struct','fields':[
        {'name':'value','type':'string','nullable':True,'metadata':{'comment':comment}},
        {'name':'count','type':'long','nullable':False,'metadata':{'origin':'project'}},
    ]}


def wrap_type(value, container):
    if container=='struct': return value
    if container=='array': return {'type':'array','elementType':value,'containsNull':True}
    if container=='map': return {'type':'map','keyType':'string','valueType':value,'valueContainsNull':True}
    return {'type':'map','keyType':'string','valueType':{'type':'array','elementType':value,'containsNull':True},'valueContainsNull':False}


@pytest.mark.parametrize('container',['struct','array','map','map_array'])
def test_nested_metadata_drift_does_not_reject_source_or_existing_target(container):
    import copy
    expected=wrap_type(nested_type('declared'),container)
    actual=wrap_type(nested_type('existing'),container)
    original=copy.deepcopy(actual)
    session=spark(); session.createDataFrame.side_effect=None
    session.createDataFrame.return_value=NS(schema=Schema([('payload',expected)]))
    session.catalog.tableExists.return_value=True
    session.table.return_value=NS(schema=Schema([('payload',actual)]))
    spec=table(columns=[ColumnSpec('payload','STRUCT<value: STRING, count: BIGINT>','Payload')])
    validate_delta_schema(NS(sparkSession=session,schema=Schema([('payload',actual)])),spec)
    assert actual==original  # Comparisons do not strip metadata from the real schema.


@pytest.mark.parametrize('change',['name','type','order','nullable','containsNull','valueContainsNull'])
def test_nested_structural_changes_still_fail_schema_comparison(change):
    import copy
    from tablepact._internal.schema import shape
    before=wrap_type(nested_type('same'),'map_array')
    after=copy.deepcopy(before)
    fields=after['valueType']['elementType']['fields']
    if change=='name': fields[0]['name']='renamed'
    if change=='type': fields[0]['type']='long'
    if change=='order': fields.reverse()
    if change=='nullable': fields[0]['nullable']=False
    if change=='containsNull': after['valueType']['containsNull']=False
    if change=='valueContainsNull': after['valueContainsNull']=True
    assert shape(Schema([('payload',before)])) != shape(Schema([('payload',after)]))


def test_native_struct_type_attributes_ignore_metadata_without_erasing_atomic_type_properties():
    from tablepact._internal.schema import shape
    class AtomicType:
        def __init__(self,collation): self.collation=collation
        def __eq__(self,other): return isinstance(other,AtomicType) and self.collation==other.collation
        def jsonValue(self): return "string"  # An atomic JSON form need not carry every property.
    def native(comment, collation="UTF8_BINARY", nullable=True):
        return NS(fields=[NS(name="value",dataType=AtomicType(collation),nullable=nullable,metadata={"comment":comment})])
    assert shape(Schema([('payload',native('old'))])) == shape(Schema([('payload',native('new'))]))
    assert shape(Schema([('payload',native('same'))])) != shape(Schema([('payload',native('same','UTF8_LCASE'))]))
    assert shape(Schema([('payload',native('same'))])) != shape(Schema([('payload',native('same',nullable=False))]))


def test_native_array_and_map_attributes_preserve_container_nullability():
    from tablepact._internal.schema import shape
    before=NS(keyType='string',valueType=NS(elementType=NS(fields=[NS(name='v',dataType='long',nullable=True,metadata={'comment':'a'})]),containsNull=True),valueContainsNull=True)
    after=NS(keyType='string',valueType=NS(elementType=NS(fields=[NS(name='v',dataType='long',nullable=True,metadata={'comment':'b'})]),containsNull=True),valueContainsNull=True)
    assert shape(Schema([('payload',before)]))==shape(Schema([('payload',after)]))
    after.valueContainsNull=False
    assert shape(Schema([('payload',before)]))!=shape(Schema([('payload',after)]))
