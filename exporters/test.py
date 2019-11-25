import os
import click


@click.command()
@click.option('--username', envvar='USERNAME', default=None, required=True, help='name of user with API access to kitos')
@click.option('--password', envvar='PASSWORD', default=None, required=True, help='User Password')
@click.option('--kitosserver', envvar='KITOSSERVER', default=None, required=True, help='Name of KITOS server')
def ShowParameters(username, password, kitosserver):
    username = username
    print(username)
    print(password)
    print(kitosserver)


if __name__ == '__main__':
    ShowParameters()
