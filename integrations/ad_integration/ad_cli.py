import click


@click.group()
@click.pass_context
def cli(ctx):
    """AD Top-level"""
    pass


@cli.command()
@click.pass_context
def noop(ctx):
    """Do nothing."""
    pass


@cli.command()
@click.option('--validate-script/--no-validate', default=True,
              help='Validate that a template can be parsed')
@click.option('--execute-script/--no-execute', default=True,
              help='Execute script with values from user')
@click.pass_context
def ad_execute(ctx, validate_script, no_validate):
    """Do nothing."""
    exe = ADExecute()


@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name',
              help='The person to greet.')
def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(count):
        click.echo('Hello %s!' % name)


if __name__ == '__main__':
    cli()
