import click
from cirro import DataPortal
from commands.run_annotate import run_annotate

def check_required_args(args):
    if any(value is None for value in args.values()):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        ctx.exit()

@click.group(help="Cirro CLI - Tool for interacting with datasets")
@click.version_option()
def run():
    pass  # Print out help text, nothing to do

@run.command(help='Annotate Pipeline Outputs', no_args_is_help=False)
def annotate(**kwargs):
    check_required_args(kwargs)
    run_annotate(kwargs)

def main():
    try:
        run()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()