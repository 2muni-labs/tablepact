"""Description commands only; no general project SQL executor or retry loop."""

from tablepact._internal.schema import quote_column, quote_target
from tablepact.errors import WriteError


def _literal(value):
    return "'" + value.replace('\\', '\\\\').replace("'", "\\'") + "'"


def description_commands(spark, spec):
    # Compile before writing. Do not silently change parsing or variable expansion.
    get = spark.conf.get
    if get('spark.sql.parser.escapedStringLiterals', 'false').strip().lower() != 'false':
        raise WriteError('Description output requires spark.sql.parser.escapedStringLiterals=false')
    descriptions = [spec.description, *(c.description for c in spec.columns)]
    if any('${' in text for text in descriptions):
        if get('spark.sql.variable.substitute', 'true').strip().lower() != 'false':
            raise WriteError('Descriptions containing ${ require spark.sql.variable.substitute=false')
    target = quote_target(spec.target)
    return [f'COMMENT ON TABLE {target} IS {_literal(spec.description)}'] + [
        f'COMMENT ON COLUMN {target}.{quote_column(c.name)} IS {_literal(c.description)}'
        for c in spec.columns
    ]


def apply_descriptions(spark, spec, commands):
    """Read post-write UC metadata once and apply only changed descriptions.

    Read errors are not treated as missing metadata. The caller reports these as
    description-stage failures after a completed data write, without blind retries.
    """
    table_description = spark.catalog.getTable(spec.target).description
    fields = {field.name: field.metadata.get("comment")
              for field in spark.table(spec.target).schema.fields}
    if any(column.name not in fields for column in spec.columns):
        raise WriteError("Post-write table is missing declared columns")
    changes = []
    if table_description != spec.description:
        changes.append(commands[0])
    for column, command in zip(spec.columns, commands[1:], strict=True):
        if fields[column.name] != column.description:
            changes.append(command)
    for command in changes:
        spark.sql(command)
