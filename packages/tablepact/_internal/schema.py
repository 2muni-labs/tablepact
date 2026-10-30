"""Native schema parsing and metadata-only compatibility checks."""

from tablepact.errors import SpecError, WriteError


def quote_column(name):
    return '`' + name.replace('`', '``') + '`'


def quote_target(name):
    """Quote multipart identifiers, preserving dots inside native backtick parts."""
    parts, part, quoted, closed = [], '', False, False
    i = 0
    while i < len(name):
        c = name[i]
        if quoted:
            if c == '`':
                if i + 1 < len(name) and name[i + 1] == '`':
                    part += '`'; i += 1
                else:
                    quoted, closed = False, True
            else:
                part += c
        elif c == '.':
            if not part:
                raise SpecError('target contains an empty identifier')
            parts.append(part); part, closed = '', False
        elif c == '`':
            if part or closed:
                raise SpecError('Invalid target identifier quoting')
            quoted = True
        else:
            if closed or c.isspace():
                raise SpecError('Invalid target identifier quoting')
            part += c
        i += 1
    if quoted or not part:
        raise SpecError('Invalid target identifier quoting')
    parts.append(part)
    return '.'.join(quote_column(p) for p in parts)


def _has_nonnull(schema):
    for field in getattr(schema, 'fields', ()):
        if not field.nullable or _has_nonnull(field.dataType):
            return True
    for name in ('elementType', 'keyType', 'valueType'):
        child = getattr(schema, name, None)
        if child is not None and _has_nonnull(child):
            return True
    return False


def _type_shape(data_type):
    """Compare native types without StructField metadata; retain nullability."""
    # Use public complex-type attributes; keep atomic native type objects so
    # attributes such as decimal precision and string collation remain significant.
    if hasattr(data_type, "fields"):
        return {"type": "struct", "fields": [
            {"name": field.name, "type": _type_shape(field.dataType), "nullable": field.nullable}
            for field in data_type.fields
        ]}
    if hasattr(data_type, "elementType"):
        return {"type": "array", "elementType": _type_shape(data_type.elementType),
                "containsNull": data_type.containsNull}
    if hasattr(data_type, "keyType"):
        return {"type": "map", "keyType": _type_shape(data_type.keyType),
                "valueType": _type_shape(data_type.valueType),
                "valueContainsNull": data_type.valueContainsNull}
    if not isinstance(data_type, dict):
        return data_type
    result = dict(data_type)
    if result.get("type") == "struct":
        result["fields"] = [
            {**{key: value for key, value in field.items() if key != "metadata"},
             "type": _type_shape(field["type"])}
            for field in result["fields"]
        ]
    elif result.get("type") == "array":
        result["elementType"] = _type_shape(result["elementType"])
    elif result.get("type") == "map":
        for key in ("keyType", "valueType"):
            result[key] = _type_shape(result[key])
    return result


def shape(schema):
    return [(field.name, _type_shape(field.dataType)) for field in schema.fields]


def declared_schema(spark, spec):
    # Only parse a schema, never execute project type text as a SQL statement.
    ddl = ', '.join(f'{quote_column(c.name)} {c.type}' for c in spec.columns)
    schema = spark.createDataFrame([], schema=ddl).schema
    if [f.name for f in schema.fields] != [c.name for c in spec.columns]:
        raise SpecError('Each column.type must define exactly one native data type')
    if _has_nonnull(schema):
        raise SpecError('NOT NULL declarations are not supported')
    # StructType JSON APIs preserve nested types without importing Spark at import time.
    data = schema.jsonValue()
    for column, field in zip(spec.columns, data['fields'], strict=True):
        field['metadata'] = dict(field.get('metadata', {}), comment=column.description)
    schema = type(schema).fromJson(data)
    if spec.flow is not None and spec.flow.type != 'append':
        types = {f.name: f.dataType for f in schema.fields}
        if _type_shape(types['__START_AT']) != _type_shape(types['__END_AT']):
            raise SpecError('SCD2 system column types must match')
    return schema


def validate_target_schema(spark, spec, declared):
    if spark.catalog.tableExists(spec.target):
        if shape(spark.table(spec.target).schema) != shape(declared):
            raise WriteError('Existing target columns/order/types do not match spec.columns')


def validate_delta_schema(df, spec):
    declared = declared_schema(df.sparkSession, spec)
    if shape(df.schema) != shape(declared):
        raise WriteError('Input columns/order/types do not match spec.columns')
    validate_target_schema(df.sparkSession, spec, declared)
    return declared
