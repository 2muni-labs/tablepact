from dataclasses import replace
from unittest.mock import Mock

import pytest

from tablepact._internal.descriptions import description_commands
from tablepact._internal.schema import quote_target
from tablepact.errors import DescriptionApplyError, SpecError, WriteError
from tablepact.delta import overwrite
from tablepact.contracts import ColumnSpec
from helpers import spark, table
from test_delta import BatchFrame


def test_description_literals_and_identifiers_do_not_become_sql_code():
    spec=table(target='`my.catalog`.`s``chema`.orders', description="Owner's \\path\n설명 ; DROP TABLE x --")
    commands=description_commands(spark(),spec)
    assert commands[0] == "COMMENT ON TABLE `my.catalog`.`s``chema`.`orders` IS 'Owner\\'s \\\\path\n설명 ; DROP TABLE x --'"
    assert commands[1] == "COMMENT ON COLUMN `my.catalog`.`s``chema`.`orders`.`id` IS 'id description'"
    col=ColumnSpec('x` . name','BIGINT','Column description')
    assert '.`x`` . name` IS ' in description_commands(spark(),table(columns=[col]))[1]


@pytest.mark.parametrize('target', ['a..b','`open','a.`closed`x','a.','', 'a `b`'])
def test_malformed_target_is_rejected_before_write(target):
    with pytest.raises(SpecError): quote_target(target)


@pytest.mark.parametrize('setting', ['true','TRUE'])
def test_unsupported_literal_mode_fails_before_writing_without_config_mutation(setting):
    frame=BatchFrame(); frame.config['spark.sql.parser.escapedStringLiterals']=setting
    with pytest.raises(WriteError,match='escapedStringLiterals'):
        overwrite(frame,spec=table(),replace_where='TRUE')
    assert frame.write_accesses==0
    assert frame.config['spark.sql.parser.escapedStringLiterals']==setting


def test_native_variable_expansion_is_never_applied_to_descriptions():
    frame=BatchFrame(); spec=table(description='Literal ${name}')
    with pytest.raises(WriteError,match='variable.substitute'):
        overwrite(frame,spec=spec,replace_where='TRUE')
    assert frame.write_accesses==0
    frame.config['spark.sql.variable.substitute']='false'
    overwrite(frame,spec=spec,replace_where='TRUE')
    assert '${name}' in frame.sparkSession.sql.call_args_list[0].args[0]


def test_descriptions_are_applied_after_native_write_and_overwrite_old_comments():
    frame=BatchFrame(); events=[]
    frame.writer.saveAsTable=Mock(side_effect=lambda _: events.append('write'))
    frame.sparkSession.sql.side_effect=lambda command: events.append(command)
    result=overwrite(frame,spec=table(),replace_where='id > 0')
    assert events[0]=='write'
    assert events[1:]==description_commands(spark(),table())
    assert result.target=='c.s.output'


@pytest.mark.parametrize('fail_at',[0,1,2])
def test_metadata_partial_failure_reports_completed_write_and_chains_native_error(fail_at):
    frame=BatchFrame(); error=RuntimeError('UC description denied')
    frame.sparkSession.sql.side_effect=[None]*fail_at+[error]
    with pytest.raises(DescriptionApplyError) as caught:
        overwrite(frame,spec=table(),replace_where='TRUE')
    assert caught.value.target=='c.s.output'
    assert caught.value.stage=='description' and caught.value.data_written is True
    assert caught.value.__cause__ is error
    assert frame.writer.calls.count(('save','c.s.output'))==1
    assert frame.sparkSession.sql.call_count==fail_at+1
    assert not any(c[0]=='rollback' for c in frame.writer.calls)


def test_write_failure_never_starts_description_update_or_wraps_write_error():
    frame=BatchFrame(); error=RuntimeError('native write failed')
    frame.writer.saveAsTable=Mock(side_effect=error)
    with pytest.raises(RuntimeError) as caught:
        overwrite(frame,spec=table(),replace_where='TRUE')
    assert caught.value is error
    frame.sparkSession.sql.assert_not_called()


def set_current_descriptions(frame,spec,*,table_description=None,column_descriptions=None):
    from types import SimpleNamespace as NS
    frame.sparkSession.catalog.getTable.return_value=NS(description=table_description)
    frame.sparkSession.table.return_value=NS(schema=NS(fields=[
        NS(name=c.name,metadata={} if column_descriptions is None else {'comment':column_descriptions[c.name]})
        for c in spec.columns
    ]))


def test_unchanged_descriptions_issue_zero_metadata_writes():
    frame=BatchFrame(); spec=table()
    set_current_descriptions(frame,spec,table_description=spec.description,
                             column_descriptions={c.name:c.description for c in spec.columns})
    overwrite(frame,spec=spec,replace_where='TRUE')
    frame.sparkSession.sql.assert_not_called()
    frame.sparkSession.catalog.getTable.assert_called_once_with(spec.target)
    frame.sparkSession.table.assert_called_once_with(spec.target)
    assert frame.writer.calls.count(('save',spec.target))==1


@pytest.mark.parametrize('changed',['table','id','processing_date'])
def test_only_changed_description_is_updated(changed):
    frame=BatchFrame(); spec=table()
    comments={c.name:c.description for c in spec.columns}
    if changed!='table': comments[changed]='Old column description'
    set_current_descriptions(frame,spec,table_description='Old table description' if changed=='table' else spec.description,
                             column_descriptions=comments)
    overwrite(frame,spec=spec,replace_where='TRUE')
    all_commands=description_commands(spark(),spec)
    index={'table':0,'id':1,'processing_date':2}[changed]
    frame.sparkSession.sql.assert_called_once_with(all_commands[index])


def test_description_diff_uses_post_write_metadata_instead_of_prewrite_snapshot():
    frame=BatchFrame(); spec=table()
    set_current_descriptions(frame,spec,table_description='Old',column_descriptions={c.name:'Old' for c in spec.columns})
    def write(_):
        set_current_descriptions(frame,spec,table_description=spec.description,
                                 column_descriptions={c.name:c.description for c in spec.columns})
    frame.writer.saveAsTable=Mock(side_effect=write)
    overwrite(frame,spec=spec,replace_where='TRUE')
    frame.sparkSession.sql.assert_not_called()


@pytest.mark.parametrize('failure',['table_description','column_metadata','missing_column'])
def test_post_write_metadata_observation_failure_is_not_a_blind_update(failure):
    from types import SimpleNamespace as NS
    frame=BatchFrame(); error=RuntimeError('Cannot read UC metadata')
    if failure=='table_description': frame.sparkSession.catalog.getTable.side_effect=error
    if failure=='column_metadata': frame.sparkSession.table.side_effect=error
    if failure=='missing_column': frame.sparkSession.table.return_value=NS(schema=NS(fields=[]))
    with pytest.raises(DescriptionApplyError) as caught:
        overwrite(frame,spec=table(),replace_where='TRUE')
    assert caught.value.data_written is True
    if failure!='missing_column': assert caught.value.__cause__ is error
    frame.sparkSession.sql.assert_not_called()
    assert frame.writer.calls.count(('save','c.s.output'))==1


def test_changed_description_command_failure_keeps_partial_failure_contract():
    frame=BatchFrame(); spec=table()
    set_current_descriptions(frame,spec,table_description=spec.description,
                             column_descriptions={'id':'old','processing_date':spec.columns[1].description})
    error=RuntimeError('MODIFY denied'); frame.sparkSession.sql.side_effect=error
    with pytest.raises(DescriptionApplyError) as caught:
        overwrite(frame,spec=spec,replace_where='TRUE')
    assert caught.value.__cause__ is error and caught.value.data_written is True
    assert frame.sparkSession.sql.call_count==1
