import subprocess
import sys


def test_imports_are_engine_independent_and_public_functions_are_explicit():
    script = '''
import importlib.abc
import importlib.util
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyspark', 'delta', 'databricks'}:
            raise AssertionError('Engine import: ' + fullname)
sys.meta_path.insert(0, Block())
import tablepact
from tablepact.delta import overwrite
from tablepact.streaming import define
from tablepact.spec import load_spec
from tablepact.contracts import TableSpec, WriteResult
assert all(callable(f) for f in (overwrite, define, load_spec))
from tablepact import delta, streaming
assert delta.overwrite is overwrite
assert streaming.define is define
for old_module in ('tablepact.job', 'tablepact.pipeline', 'tablepact._internal.job', 'tablepact._internal.pipeline'):
    assert importlib.util.find_spec(old_module) is None
assert importlib.util.find_spec('tablepact.view') is None
assert importlib.util.find_spec('tablepact.types') is None
assert importlib.util.find_spec('tablepact._internal.spec') is None
assert not hasattr(tablepact, 'publish')
assert not hasattr(TableSpec, 'from_yaml')
import tablepact.streaming as streaming
assert not hasattr(streaming, 'define_snapshot_scd2')
assert not hasattr(tablepact, 'resolve_parameters')
assert importlib.util.find_spec('tablepact.parameters') is None
assert importlib.util.find_spec('tablepact._internal.parameters') is None
import tablepact.contracts as contracts
import tablepact.errors as errors
assert not hasattr(contracts, 'ProcessingWindow')
assert not hasattr(contracts, 'ResolvedParameters')
assert not hasattr(errors, 'ParameterError')
assert importlib.util.find_spec('tablepact.lakeflow') is None
assert importlib.util.find_spec('tablepact._internal.publish') is None
assert importlib.util.find_spec('tablepact._internal.lakeflow') is None
from tablepact.errors import WriteError
assert not any(k.split('.')[0] in {'pyspark', 'delta', 'databricks'} for k in sys.modules)
'''
    subprocess.run([sys.executable, '-I', '-c', script], check=True)
