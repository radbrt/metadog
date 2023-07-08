from yaml import safe_load
from dotenv import dotenv_values, load_dotenv
import os
import jinja2
from .setup import run_model_ddls
import click
from .file_utils import profile_files
from .db_utils import profile_db
from .analyze import analyze_table

load_dotenv()


def parse_spec():
    spec_txt = open('metadog.yaml', 'r').read()
    jinja_parsed = jinja2.Template(spec_txt).render(dotenv_values())

    spec = safe_load(jinja_parsed)

    return spec


def write_metadata(scan_payload):
    raise NotImplementedError("Not implemented yet")


def setup_backend():
    run_model_ddls()


@click.group()
def metadog():
    pass


@metadog.command()
@click.argument('foldername')
def init(foldername: str):
    """
    Initialize a new metadog project in the specified folder
    Should it initialize the database as well? A flag to not do that?
    """
    raise NotImplementedError("Not implemented yet")


@metadog.command()
@click.option('--select', '-s', help='Select sources to scan')
@click.option('--no-stats', help='Omit generating table statistics', is_flag=True)
def scan(select, no_stats):
    project_spec = parse_spec()

    scan_result = {"sources": []}

    for source in project_spec["sources"]:
        source_dict = {"name": source["name"]}
        
        match source["type"]:
            case "database" :
                print(f"Scanning database {source['name']}")
                for db in source["databases"]:
                    config = source['connection']
                    # config["database"] = db
                    do_scan = source.get("analyze", True) and not no_stats

                    p, s = profile_db(source.get('flavor'), db, config, do_scan)
                    print(p)
                    print("--------------------------")
                    print(s)

            case "database":
                print(f"Scanning database {source['name']}")
                for db in source["databases"]:
                    config = source['connection']
                    # config["database"] = db
                    do_scan = source.get("analyze", True) and not no_stats
                    p = profile_db(source.get('flavor'), db, config, do_scan)
                   
            case "sftp":
                print(f"Scanning sftp {source['name']}")
                if source.get("get_schemas", True):
                    print(f"Getting schemas for sftp {source['name']}")
                    schemas = profile_files('sftp', filetype='csv', get_schema=True, n_samples=1000,
                                  host=source['host'], username=source['username'], password=source['password'])
                    print(schemas)
            case _:
                raise NotImplementedError("Source type not implemented")
        scan_result["sources"].append(source_dict)

@metadog.command()
def warnings():
    raise NotImplementedError("Not implemented yet")


@metadog.command()
def hello():
    print(
        """
           _____  ___________________________  ________   ________    ________     / \__
          /     \ \_   _____/\__    ___/  _  \ \______ \  \_____  \  /  _____/    (    @\__ 
         /  \ /  \ |    __)_   |    | /  /_\  \ |    |  \  /   |   \/   \  ___    /         O
        /    Y    \|        \  |    |/    |    \|    `   \/    |    \    \_\  \   /   (_____/
        \____|__  /_______  /  |____|\____|__  /_______  /\_______  /\______  /   /_____/  U
                \/        \/                 \/        \/         \/        \/    
        """
    )
