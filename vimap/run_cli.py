import click
import os
from vimap import VERSION
PRJ_DIR = os.getcwd()


@click.group()
def entry_point():
    pass


@click.command()
def version():
    print(f"Vimap version: {VERSION}")


entry_point.add_command(version)


if __name__ == "__main__":
    pass