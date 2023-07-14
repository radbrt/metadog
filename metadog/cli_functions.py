from yaml import safe_load
from dotenv import dotenv_values, load_dotenv
import os
import jinja2
from .setup import run_model_ddls
from metadog.connection_handlers.sftp_connection import SFTPFileSystem
from metadog.file_handlers.csv_handler import CSVHandler
from metadog.backend_handlers import GenericBackendHandler
from metadog.db_scanners import GenericDBScanner
from metadog.db_scanners.snowflake_scanner import SnowflakeScanner
from dotenv import load_dotenv
from metadog.outlierdetection import OutlierDetector


load_dotenv()


def parse_spec():
    spec_txt = open('metadog.yaml', 'r').read()
    jinja_parsed = jinja2.Template(spec_txt).render(dotenv_values())
    if not os.getenv("METADOG_BACKEND_URI"):
        if 'METADOG_BACKEND_URI' in dotenv_values():
            backend_uri = dotenv_values()['METADOG_BACKEND_URI'] or 'sqlite:///metadog.db'

            os.environ["METADOG_BACKEND_URI"] = backend_uri
        else:
            raise Exception("METADOG_BACKEND_URI not set")
    spec = safe_load(jinja_parsed)

    return spec


def write_metadata(scan_payload):
    raise NotImplementedError("Not implemented yet")


def setup_backend():
    run_model_ddls()


def init_fn(foldername):
    """
    Initialize a new metadog project in the specified folder.
    """

    current_script_dir = os.path.dirname(__file__)
    source_file_loc = os.path.join(current_script_dir, 'template', 'metadog.template')
    source_file = open(source_file_loc, 'r')
    source_file_string = source_file.read()
    source_file.close()


    pth = os.path.join(foldername, 'metadog.yaml')
    pwd = os.getcwd()
    if os.path.exists(pth):
        raise Exception(f"metadog.yaml already exists in {foldername}")
    else:
        full_path = os.path.join(pwd, pth)
        os.makedirs(foldername, exist_ok=False)
        # Create file
        with open(full_path, 'w') as f:
            f.write(source_file_string)

    # raise NotImplementedError("Not implemented yet")


def scan_fn(select, no_stats):
    """
    Main scan function, parses the metadog.yaml file, scans the specified sources
    and writes the results to the backend
    """
    
    project_spec = parse_spec()
    connection_uri = os.getenv("METADOG_BACKEND_URI")
    if connection_uri:
        backend = GenericBackendHandler(connection_uri=connection_uri)
    else:
        backend = GenericBackendHandler()


    for source in project_spec["sources"]:

        match source["type"]:

            case "snowflake":
                print(f"Scanning snowflake {source['name']}")
                for db in source["databases"]:
                    config = source['connection']
                    do_analyze = source.get("analyze", True) and not no_stats

                    db_scanner = SnowflakeScanner(database=db, **config)

                    catalog, stats = db_scanner.profile_db(db, do_analyze)

                    backend.merge_database_crawl(domain=source['name'], db_json=catalog)
                    if do_analyze:
                        backend.merge_database_stats(domain=source['name'], db_json=stats)

            case "database" :
                print(f"Scanning database {source['name']}")
                for db in source["databases"]:
                    config = source['connection']
                    do_analyze = source.get("analyze", True) and not no_stats

                    db_scanner = GenericDBScanner(database=db, **config)

                    catalog, stats = db_scanner.profile_db(db, do_analyze)

                    backend.merge_database_crawl(domain=source['name'], db_json=catalog)
                    if do_analyze:
                        backend.merge_database_stats(domain=source['name'], db_json=stats)

            case "sftp":
                print(f"Scanning sftp {source['name']}")
                get_schemas = source.get("get_schemas", False)
                filesystem = SFTPFileSystem(host=source['host'], username=source['username'], password=source['password'])
                all_files = filesystem.get_files()
                highwater = backend.get_last_modified(server=source['name'])
                files = [f['name'] for f in all_files if f['mtime'] > highwater]
                schemas = []
                for file_name in files:
                    if file_name.endswith('.csv'):
                        print(file_name)
                        file_stream = filesystem.get_file(file_name)
                        csv_handler = CSVHandler(file_stream, file_name, get_schema=get_schemas)
                        schemas.append(csv_handler.get_file_metadata())
                    else:
                        schemas.append({"file": file_name, "properties": {} })
                
                file_domain = filesystem.uri
                backend.merge_file_crawl(domain=source['name'], protocol='sftp', file_list=schemas)
                last_modified = filesystem.get_last_modified()
                backend.register_scan(server=source['name'], last_modified=last_modified)

            case _:
                raise NotImplementedError("Source type not implemented")

def warnings_fn():
    """
    Print warnings about the current project
    """
    project_spec = parse_spec()
    connection_uri = os.getenv("METADOG_BACKEND_URI")
    if connection_uri:
        backend = GenericBackendHandler(connection_uri=connection_uri)
    else:
        backend = GenericBackendHandler()

    analyzer = OutlierDetector()
    partitions = backend.get_partitions()

    n_outier_partitions = 0
    for partition in partitions:
        df = backend.get_partition(partition)

        outliers = analyzer.get_outliers_in_df(df)
        if len(outliers) > 0:
            print(f"Outliers found in metric URI {partition}")
            print(outliers)
            n_outier_partitions += 1

    if n_outier_partitions == 0:
        print("No outliers found")
