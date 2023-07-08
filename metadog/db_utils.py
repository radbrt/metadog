import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from snowflake.sqlalchemy import URL as sfURL
from .analyze import analyze_table


def get_db_uri(db_type: str, **kwargs) -> URL | str:
    """
    Returns a database URI for sqlalchemy to connect to
    """
    if db_type == 'postgres':
        kwargs["port"] = kwargs.get('port') or 5432
        kwargs["drivername"] = kwargs.get('drivername') or 'postgresql'
        pgurl = URL.create(**kwargs)

        return pgurl

    elif db_type == 'mysql':
        kwargs["port"] = kwargs.get('port') or 5432
        kwargs["drivername"] = kwargs.get('drivername') or 'mysql+pymysql'
        mysqlurl = URL.create(**kwargs)

        return mysqlurl

    elif db_type == 'snowflake':
        sfurl = sfURL(**kwargs)
        return sfurl

    else:
        raise NotImplementedError("Database type not implemented")


def get_table_schema(schema_name, table_name: str, engine) -> str:
    inspector = sqlalchemy.inspect(engine)
    tbl_schema = inspector.get_columns(schema=schema_name, table_name=table_name)
    return tbl_schema


def get_tables_in_schema(schema: str, engine) -> list:
    inspector = sqlalchemy.inspect(engine)
    tables = inspector.get_table_names(schema=schema)
    return tables


def get_schemas_in_db(engine) -> list:
    inspector = sqlalchemy.inspect(engine)
    schemas = inspector.get_schema_names()
    return schemas


def convert_schema_to_singer(schema):
    """
    Convert a schema from sqlalchemy to a singer schema.
    """
    singer_schema = {
        "type": ["object"],
        "properties": {},
        "additionalProperties": False,
    }

    for column in schema:
        singer_schema["properties"][column["name"]] = {
            "type": str(column["type"]),
        }

    return singer_schema


def convert_schema_to_openlineage(schema, namespace, name):
    """
    Convert a schema from sqlalchemy to an openlineage schema.
    """
    openlineage_schema = {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }

    for column in schema:
        openlineage_schema["properties"][column["name"]] = {
            "type": str(column["type"].as_generic()),
        }

    return openlineage_schema


def profile_db(db_flavor, db_name, connection_info, do_scan) -> tuple:
    """
    Profile a database and return a list of singer schemas.
    """
    connection_info["database"] = db_name
    db_uri = get_db_uri(db_flavor, **connection_info)
    engine = create_engine(db_uri)

    schemas = get_schemas_in_db(engine)
    full_scan = {"database": db_name, "schemas": {}}
    all_stats = {"database": db_name, "stats": []}
    for schema in schemas:
        tables = get_tables_in_schema(schema, engine)
        tbl_schemas = []
        for table in tables:
            print(f"Getting {table} from {schema}")
            tbl_schema = get_table_schema(schema, table, engine)
            gotten_table_schema = convert_schema_to_singer(tbl_schema)
            gotten_table_schema["name"] = table
            tbl_schemas.append(gotten_table_schema)
            if do_scan:
                stats = analyze_table(tbl_name=table, schema=schema, engine=engine)
                all_stats["stats"].append({"table": table, "schema": schema, "stats": stats})

        full_scan["schemas"][schema] = tbl_schemas

    return full_scan, all_stats

