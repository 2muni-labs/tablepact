"""Native API doubles. They do not claim real Spark/Delta/Lakeflow behavior."""

import copy
import re
from types import SimpleNamespace as NS
from unittest.mock import Mock

from tablepact.contracts import AppendFlowSpec, CDCFlowSpec, ColumnSpec, SnapshotFlowSpec, TableSpec


class Schema:
    def __init__(self, columns):
        self.fields = [NS(name=n, dataType=t, nullable=True, metadata={}) for n, t in columns]

    def fieldNames(self):
        return [f.name for f in self.fields]

    def jsonValue(self):
        return {'fields': [dict(name=f.name, type=f.dataType, nullable=f.nullable,
                                metadata=copy.deepcopy(f.metadata)) for f in self.fields]}

    @classmethod
    def fromJson(cls, data):
        obj = cls([])
        obj.fields = [NS(name=f['name'], dataType=f['type'], nullable=f['nullable'], metadata=f['metadata']) for f in data['fields']]
        return obj


def spark():
    def parse(rows, *, schema):
        assert rows == []
        # Fixtures use simple types; native complex-type parsing is not simulated.
        matches = re.findall(r'`((?:``|[^`])+)`\s+([^,]+)(?:,\s*|$)', schema)
        return NS(schema=Schema([(n.replace('``', '`'), t.strip().lower()) for n, t in matches]))
    conf = {}
    return NS(conf=NS(get=Mock(side_effect=lambda key, default: conf.get(key, default))),
              config=conf, createDataFrame=Mock(side_effect=parse),
              catalog=NS(tableExists=Mock(return_value=False), getTable=Mock(return_value=NS(description=None))),
              table=Mock(), sql=Mock())


def table(kind='delta', flow=None, **kwargs):
    columns = [('id', 'BIGINT'), ('processing_date', 'DATE')]
    if kind == 'streaming':
        columns += [('snapshot_date', 'DATE'), ('status', 'STRING')]
        if not isinstance(flow, AppendFlowSpec):
            columns += [('__START_AT', 'BIGINT'), ('__END_AT', 'BIGINT')]
    args = dict(kind=kind, target='c.s.output', description='Output description',
                columns=tuple(ColumnSpec(n, t, n + ' description') for n, t in columns), flow=flow)
    return TableSpec(**(args | kwargs))


def streaming_spec(kind, **kwargs):
    flow = {'snapshot_scd2': SnapshotFlowSpec(2), 'cdc_scd2': CDCFlowSpec(2), 'append': AppendFlowSpec()}[kind]
    return table(**(dict(kind='streaming', flow=flow) | kwargs))
