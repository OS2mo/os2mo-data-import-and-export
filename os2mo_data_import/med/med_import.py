import asyncio
import json
import sys
from functools import partial
from uuid import UUID
from functools import wraps

import aiohttp
import click

from utils import async_to_sync, dict_map, consume
from schema import validate_payload


def is_uuid(string: str) -> bool:
    try:
        UUID(string)
        return True
    except ValueError:
        return False


def is_uuid_list(listy: list) -> bool:
    return all(list(map(is_uuid, listy)))


def ensure_session(func):
    async def _decorator(self, *args, **kwargs):
        if self.session:
            return await func(self, session, *args, **kwargs)
        else:
            async with aiohttp.ClientSession() as session:
                return await func(self, session, *args, **kwargs)
    return _decorator


class MoxHelper:
    def __init__(self, hostname, session=None):
        self.hostname = hostname
        self.session = session

    @ensure_session
    async def _search(self, session, service, obj, params):
        url = f"{self.hostname}/{service}/{obj}"
        async with session.get(url, params=params) as response:
            data = await response.json()
            return data["results"][0]

    @ensure_session
    async def _create(self, session, service, obj, payload):
        validate_payload(payload, obj)
        url = f"{self.hostname}/{service}/{obj}"
        async with session.post(url, json=payload) as response:
            return await response.json()

    async def read_all_facets(self):
        result = await self._search("klassifikation", "facet", {"bvn": "%"})
        # Check that we got back valid UUIDs
        if not is_uuid_list(result):
            raise ValueError("Endpoint did not return a list of uuids")
        return result

    async def create_facets(self, facets):
        for facet in facets:
            result = await self._create("klassifikation", "facet", facet)
            print(result)


def session_getter(session, base, url, *args, **kwargs):
    url = f"{base}/{url}"
    return session.get(url, *args, **kwargs)


async def fetch_all_facet_uuids(getter):
    url = "klassifikation/facet"
    params = {"bvn": "%"}
    async with getter(url, params=params) as response:
        data = await response.json()
        uuid_list = data["results"][0]
        # Check that we got back valid UUIDs
        try:
            consume(map(UUID, uuid_list))
        except ValueError:
            print("Expected UUIDs")
        return uuid_list


async def fetch_facets(getter, uuid_list):
    url = "klassifikation/facet"
    params = list(map(lambda uuid: ("uuid", uuid), uuid_list))
    async with getter(url, params=params) as response:
        data = await response.json()
        facet_list = data["results"][0]
        return facet_list


def generate_facet(bvn, org):
    from payloads import lora_facet
    return lora_facet(bvn, org)


@click.group(invoke_without_command=True)
@click.option(
    "--settings-file", default="settings.json", type=click.Path(resolve_path=True)
)
@click.pass_context
def settings_loader(ctx, settings_file):
    #    # Not available until click 8
    #    settings_file_from_default = (
    #        ctx.get_parameter_source("settings-file") == click.ParameterSource.DEFAULT
    #    )
    settings_file_from_default = True
    settings = {}
    try:
        with click.open_file(settings_file, 'rb') as f:
            settings = json.load(f)
    except FileNotFoundError as exp:
        # Only default file is allowed not to exist
        if not settings_file_from_default:
            raise exp

    def fix_key(key, value):
        return key.replace(".", "_"), value

    settings = dict_map(fix_key, settings)
    # Loaded settings are default for cli
    ctx.default_map = {"cli": settings}


@settings_loader.group()
@click.option("--mox-base", default="http://localhost:8080", show_default=True)
@click.pass_context
def cli(ctx, mox_base):
    ctx.ensure_object(dict)
    ctx.obj["mox.base"] = mox_base


@cli.command()
@click.pass_context
@click.option("--facet", help="Number of greetings.")
@click.option("--bvn", help="The person to greet.")
@click.option("--title", help="The person to greet.")
@click.option("--organisation", help="The person to greet.")
@click.option("--scope", help="The person to greet.")
@async_to_sync
async def create_class(ctx, facet, bvn, title, organisation, scope):
    async with aiohttp.ClientSession() as session:
        getter = partial(session_getter, session, ctx.obj["mox.base"])
        uuid_list = await fetch_all_facet_uuids(getter)
        print(uuid_list)
        facets_list = await fetch_facets(getter, uuid_list)
        print(json.dumps(facets_list, indent=4))


@cli.command()
@click.pass_context
@click.option("--bvn", "--brugervendt-nøgle", required=True, help="The person to greet.")
@click.option("--org", "--organisation", help="The person to greet.")
@async_to_sync
async def create_facet(ctx, bvn, org):
    mox_helper = MoxHelper(ctx.obj["mox.base"])
    facet = generate_facet(bvn, org)
    result = await mox_helper.create_facets([facet])


@cli.command()
@click.option(
    "--exit-print-on-success",
    show_default=True,
    type=click.STRING,
    help="String to print if able to connect",
)
@click.option(
    "--exit-print-on-error",
    show_default=True,
    default="Unable to connect",
    type=click.STRING,
    help="String to print if unable to connect",
)
@click.option(
    "--exit-code-on-success",
    show_default=True,
    default=0,
    type=click.IntRange(0, 255),
    help="Exit code if able to connect",
)
@click.option(
    "--exit-code-on-error",
    show_default=True,
    default=1,
    type=click.IntRange(0, 255),
    help="Exit code if unable to connect",
)
@click.pass_context
def check_connection(
    ctx,
    exit_print_on_success,
    exit_print_on_error,
    exit_code_on_success,
    exit_code_on_error,
):
    """Check whether a connection can be established to mox."""
    output_map = {
        False: {"exit_code": exit_code_on_error, "message": exit_print_on_error, "color": "red"},
        True: {"exit_code": exit_code_on_success, "message": exit_print_on_success, "color": "green"},
    }

    @async_to_sync
    async def is_up():
        try:
            async with aiohttp.ClientSession() as session:
                getter = partial(session_getter, session, ctx.obj["mox.base"])
                url = "version"
                async with getter(url) as response:
                    data = await response.json()
                    if "lora_version" in data:
                        return True
        except Exception:
            pass
        return False

    output = output_map[is_up()]
    if output["message"]:
        click.secho(output["message"], fg=output["color"])
    sys.exit(output["exit_code"])


@cli.command()
@click.pass_context
def noop(ctx):
    """Do nothing."""
    pass


if __name__ == "__main__":
    settings_loader(auto_envvar_prefix="MOXUTIL")
