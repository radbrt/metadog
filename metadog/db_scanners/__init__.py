from sqlalchemy import create_engine, MetaData, Table, select, func, Numeric, Integer, String, distinct, inspect
from sqlalchemy.engine import URL
from metadog.json_schema import generate_schema, pick_datatype, infer_datatype, count_sample, infer_datatype, count_sample, generate_schema, pick_datatype, convert_schema_to_singer

class GenericDBScanner():

    def __init__(self, host, username, password, drivername, port, database, options={}) -> None:
        self.host = host
        self.username = username
        self.password = password
        self.drivername = drivername
        self.port = port
        self.database = database
        self.options = options

        self.engine = self._connect()


    def _connect(self):
        url = URL(
            drivername=self.drivername,
            host=self.host,
            username=self.username,
            password=self.password,
            port=self.port,
            database=self.database,
            query=self.options
        )

        engine = create_engine(url)

        return engine


    @property
    def base_uri(self):
        return f"{self.drivername}://{self.host}"


    def analyze_table(self, tbl_name, schema):

        metadata = MetaData(bind=self.engine, schema=schema)
        table = Table(tbl_name, metadata, autoload=True)

        # Get the numeric columns
        numeric_columns = [column for column in table.columns if isinstance(column.type, (Numeric, Integer))]
        char_columns = [column for column in table.columns if isinstance(column.type, String)]
        # Generate min, avg, and max values for each numeric column
        numeric_selects = []
        for column in numeric_columns:
            numeric_selects += [
                func.min(column).label(f"{column.name}__min"),
                func.avg(column).label(f"{column.name}__avg"),
                func.max(column).label(f"{column.name}__max"),
                func.count(column).label(f"{column.name}__null_count")
            ]

        char_selects = []
        for column in char_columns:
            char_selects += [
                func.count(distinct(column)).label(f"{column.name}__unique_count"),
                func.count(column).label(f"{column.name}__null_count")
            ]

        all_selects = numeric_selects + char_selects + [func.count()]
        stmt = select(all_selects)
        result = self.engine.execute(stmt)

        result_dict = [dict(row) for row in result]

        return result_dict
    

    def get_table_schema(self, schema_name, table_name: str) -> str:
        inspector = inspect(self.engine)
        tbl_schema = inspector.get_columns(schema=schema_name, table_name=table_name)
        return tbl_schema


    def get_tables_in_schema(self, schema: str) -> list:
        inspector = inspect(self.engine)
        tables = inspector.get_table_names(schema=schema)
        return tables


    def get_schemas_in_db(self) -> list:
        inspector = inspect(self.engine)
        schemas = inspector.get_schema_names()
        return schemas


    def profile_db(self, db_name, do_scan) -> tuple:
        """
        Profile a database and return a list of singer schemas.
        """

        schemas = self.get_schemas_in_db()
        full_scan = {"database": db_name, "schemas": {}}
        all_stats = {"database": db_name, "stats": []}
        for schema in schemas:
            tables = self.get_tables_in_schema(schema)
            tbl_schemas = []
            for table in tables:
                print(f"Getting {table} from {schema}")
                tbl_schema = self.get_table_schema(schema, table)
                gotten_table_schema = convert_schema_to_singer(tbl_schema)
                gotten_table_schema["name"] = table
                tbl_schemas.append(gotten_table_schema)
                if do_scan:
                    stats = self.analyze_table(tbl_name=table, schema=schema)
                    all_stats["stats"].append({"table": table, "schema": schema, "stats": stats})

            full_scan["schemas"][schema] = tbl_schemas

            # merge_database_crawl(db_name, full_scan)
            # merge_database_stats(db_name, all_stats)

        return full_scan, all_stats
