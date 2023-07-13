

import click
from metadog.cli_functions import init_fn, scan_fn, warnings_fn


@click.group()
def metadog():
    """Main metadog command entrypoint"""
    pass


@metadog.command()
@click.argument('foldername')
def init(foldername: str):
    """
    Initialize a new metadog project in the specified folder
    Should it initialize the database as well? A flag to not do that?
    """
    init_fn(foldername)
    


@metadog.command()
@click.option('--select', '-s', help='Select sources to scan')
@click.option('--no-stats', help='Omit generating table statistics', is_flag=True)
def scan(select, no_stats):
    scan_fn(select, no_stats)


@metadog.command()
def warnings():
    warnings_fn()


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
