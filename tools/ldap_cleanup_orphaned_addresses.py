import argparse
import json
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path

import requests
import yaml

SETTINGS_KEY = "crontab.LDAP_CLEANUP_ORPHANED_ADDRESSES_INTEGRATIONS"
SETTINGS_ENV_KEYS = (
    "LDAP_CLEANUP_ORPHANED_ADDRESSES_INTEGRATIONS",
    "crontab_LDAP_CLEANUP_ORPHANED_ADDRESSES_INTEGRATIONS",
    "CRONTAB_LDAP_CLEANUP_ORPHANED_ADDRESSES_INTEGRATIONS",
)
INIT_CONFIG_DIRECTORY = Path("/opt/docker/os2mo_ldap_import_export")
AD_NAME_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")


def load_integrations(
    settings_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[dict[str, str | int]]:
    environ = os.environ if environ is None else environ
    integration_settings = next(
        (environ[key] for key in SETTINGS_ENV_KEYS if key in environ), None
    )
    if integration_settings is not None:
        try:
            integrations = json.loads(integration_settings)
        except json.JSONDecodeError as error:
            raise ValueError("LDAP integration setting must be a JSON list") from error
    elif settings_path is not None and settings_path.is_file():
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        if not isinstance(settings, dict):
            raise ValueError("Customer settings must be a JSON object")
        integrations = settings.get(SETTINGS_KEY, [])
    else:
        integrations = []

    if integrations is None:
        integrations = []
    if not isinstance(integrations, list):
        raise ValueError(f"{SETTINGS_KEY} must be a list")

    validated = []
    names = set()
    for integration in integrations:
        if not isinstance(integration, dict):
            raise ValueError("Each LDAP integration must be an object")

        ad_name = integration.get("ad_name")
        port = integration.get("port")
        if (
            not isinstance(ad_name, str)
            or ad_name in {".", ".."}
            or not AD_NAME_PATTERN.fullmatch(ad_name)
        ):
            raise ValueError(
                "LDAP integration ad_name must contain only letters, numbers, '_', '.' or '-'"
            )
        if (
            isinstance(port, bool)
            or not isinstance(port, int)
            or not 1 <= port <= 65535
        ):
            raise ValueError(f"LDAP integration {ad_name} must have a valid port")
        if ad_name in names:
            raise ValueError(f"Duplicate LDAP integration ad_name: {ad_name}")

        names.add(ad_name)
        validated.append({"ad_name": ad_name, "port": port})
    return validated


def load_address_type_user_keys(init_config_path: Path) -> list[str]:
    config = yaml.safe_load(init_config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError(f"{init_config_path} must contain a YAML object")

    facets = config.get("facets")
    employee_address_types = (
        facets.get("employee_address_type") if isinstance(facets, dict) else None
    )
    if not isinstance(employee_address_types, dict) or not employee_address_types:
        raise ValueError(
            f"{init_config_path} must define at least one facets.employee_address_type"
        )
    if not all(isinstance(key, str) for key in employee_address_types):
        raise ValueError(
            f"{init_config_path} employee_address_type keys must be strings"
        )
    return list(employee_address_types)


def cleanup_orphaned_addresses(
    ad_name: str, port: int, config_directory: Path = INIT_CONFIG_DIRECTORY
) -> None:
    config_path = config_directory / f"init.config.{ad_name}.yml"
    address_type_user_keys = load_address_type_user_keys(config_path)
    params = [("dry_run", "false")] + [
        ("address_type_user_keys", key) for key in address_type_user_keys
    ]
    response = requests.post(
        f"http://localhost:{port}/fixup/cleanup_orphaned_addresses",
        params=params,
    )
    response.raise_for_status()
    if response.text:
        print(response.text)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list-integrations", action="store_true")
    parser.add_argument("--ad-name")
    parser.add_argument("--port", type=int)
    parser.add_argument("--config-directory", type=Path, default=INIT_CONFIG_DIRECTORY)
    args = parser.parse_args()

    try:
        if args.list_integrations:
            settings_path = os.environ.get("CUSTOMER_SETTINGS")
            for integration in load_integrations(
                Path(settings_path) if settings_path is not None else None
            ):
                print(f"{integration['ad_name']}\t{integration['port']}")
            return 0

        if args.ad_name is None or args.port is None:
            parser.error("--ad-name and --port are required unless listing integrations")
        if args.ad_name in {".", ".."} or not AD_NAME_PATTERN.fullmatch(args.ad_name):
            raise ValueError(
                "ad_name must contain only letters, numbers, '_', '.' or '-'"
            )
        if not 1 <= args.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        cleanup_orphaned_addresses(args.ad_name, args.port, args.config_directory)
    except (
        KeyError,
        OSError,
        ValueError,
        yaml.YAMLError,
        requests.RequestException,
    ) as error:
        print(f"LDAP orphaned-address cleanup failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
