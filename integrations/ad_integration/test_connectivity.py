import logging

import click
import requests
import requests_kerberos
from click_option_group import optgroup
from click_option_group import RequiredMutuallyExclusiveOptionGroup
from ra_utils.load_settings import load_settings
from winrm.exceptions import InvalidCredentialsError

from .ad_common import AD
from .ad_logger import start_logging
from .ad_reader import ADParameterReader
from .read_ad_conf_settings import read_settings


SETTINGS = load_settings()

logger = logging.getLogger("AdTestConnectivity")


def test_basic_connectivity():
    ad = AD()
    method = ad.all_settings["primary"]["method"]
    print(f"Test basic connectivity (method={method})")
    session = ad.session

    try:
        r = session.run_cmd("ipconfig", ["/all"])
        error = None
    except requests_kerberos.exceptions.KerberosExchangeError as e:
        error = str(e)
    except InvalidCredentialsError as e:
        error = "Credentails not accepted by remote management server: {}"
        error = error.format(e)
    except requests.exceptions.ConnectionError as e:
        error = "Unable to contact winrm_host {}, message: {}"
        error = error.format(SETTINGS["integrations.ad.winrm_host"], e)

    if error is None and r.status_code == 0:
        return True
    else:
        print("Error")
        if error is None:
            print(r.std_err)
        else:
            print(error)
        return False


def test_ad_contact(index):
    try:
        ad_reader = ADParameterReader(index=index)
    except Exception as e:
        print("Unable to initiate AD-reader: {}".format(e))
        return False
    response = ad_reader.read_encoding()
    if "WindowsCodePage" in response:
        return True
    else:
        print("Error:")
        print(response)
        return False


def test_full_ad_read(index):
    """
    Test that we can read actual users from AD. This ensures suitable rights
    to access cpr-information in AD.
    """
    ad_reader = ADParameterReader(index=index)

    ad_reader.uncached_read_user(cpr="3111*")
    if ad_reader.results:
        print("Found users with bithday 31. November!")
        return False

    ad_reader.uncached_read_user(cpr="30*")
    if not ad_reader.results:
        print("No users found with bithday on the 30th of any month!")
        return False

    test_chars = {"æ": False, "ø": False, "å": False, "@": False}
    # Run through a set of users and test encoding and cpr-separator.
    for user in ad_reader.results.values():
        cprfield = ad_reader.all_settings["primary"]["cpr_field"]
        cpr = user[cprfield]
        separator = ad_reader.all_settings["primary"]["cpr_separator"]
        cpr_ok = False
        if separator == "" and len(cpr) == 10:
            cpr_ok = True
        if cpr.count(separator) == 1 and len(cpr) == 11:
            cpr_ok = True
        if not cpr.replace(separator, "").isdigit():
            cpr_ok = False
        if not cpr_ok:
            msg = "CPR-is not valid according to settings: {}, cpr-length: {}"
            print(msg.format(user["Name"], len(cpr)))
            return False

        for value in user.values():
            for char in test_chars.keys():
                if str(value).find(char) > -1:
                    test_chars[char] = True

    for test_value in test_chars.values():
        if not test_value:
            print("Finding occurences of special chars: {}".format(test_chars))
            print("(The more True the better)")
            break

    return True


def test_ad_write_settings():
    all_settings = read_settings()
    if not all_settings["primary_write"]:
        return False
    else:
        return True


def perform_read_test():
    status = "Success"
    basic_connection = test_basic_connectivity()
    if not basic_connection:
        print("Unable to connect to management server")
        exit(1)

    for index in range(len(SETTINGS["integrations.ad"])):
        print("Test AD {} contact".format(index))
        ad_connection = test_ad_contact(index)
        if not ad_connection:
            print("Unable to connect to AD {}".format(index))
            status = "Failure!!"
            continue

        print("Test ability to read from AD {}".format(index))
        full_ad_read = test_full_ad_read(index)
        if not full_ad_read:
            print("Unable to read users from AD {} correctly".format(index))
            status = "Failure!!!"
            continue

    print(status)
    # if status != "Success":
    #     exit(1)


def perform_write_test():
    ad_reader = ADParameterReader()

    print("Test that AD write settings are set up")
    write_settings = test_ad_write_settings()
    if not write_settings:
        print("Write settings not correctly set up")
        exit(1)

    # TODO: If we could make a test write, it would be nice.

    print("Write settings set up correctly")

    user_info_list = []
    i = 0
    logger.info("Check for availability of needed fields - find some test users")
    print("Check for availability of needed fields - find some test users")

    minimum_expected_fields = {
        SETTINGS["integrations.ad.write.uuid_field"]: False,
        SETTINGS["integrations.ad.write.level2orgunit_field"]: False,
        SETTINGS["integrations.ad.write.org_unit_field"]: False,
    }

    while len(user_info_list) < 10:
        i = i + 1
        if i > 31:
            msg = "Cannot find 10 users - unable to test for available fields"
            print(msg)
            logger.error(msg)
            exit(1)
        msg = "So far found {} users, current cpr: {}"
        logger.info(msg.format(len(user_info_list), str(i).zfill(2)))
        user_info = ad_reader.get_from_ad(cpr=str(i).zfill(2) + "*")
        user_info_list = user_info_list + user_info

    msg = "Found more than 10 users ({}) - now test for available fields"
    print(msg.format(len(user_info_list)))
    logger.info(msg.format(len(user_info_list)))

    for user in user_info_list:
        if isinstance(user, str):
            print("Unable to read from AD: {}".format(user))
            exit(1)

        for prop in user["PropertyNames"]:
            if prop in minimum_expected_fields:
                minimum_expected_fields[prop] = True

    if not all(minimum_expected_fields.values()):
        print("An import field is now found on the tested users")
        print(minimum_expected_fields)
    else:
        print("Test of AD fields for writing is a success")


def check_settings(settings):
    if not isinstance(settings.get("integrations.ad"), list):
        raise Exception("integrations.ad skal angives som en liste af dicts (settings)")

    if len(settings.get("integrations.ad")) < 1:
        raise Exception("integrations.ad skal indeholde mindst 1 AD (settings)")

    winrm_host_setting = "integrations.ad.winrm_host"
    winrm_host_value = settings.get(winrm_host_setting)
    if winrm_host_value is None:
        raise Exception("%r is missing" % winrm_host_setting)


@click.command(help="Test AD connectivity")
@optgroup.group("Action", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option("--test-read-settings", is_flag=True)
@optgroup.option("--test-write-settings", is_flag=True)
def cli(**args):
    if args.get("test_read_settings"):
        perform_read_test()
    if args.get("test_write_settings"):
        perform_read_test()
        perform_write_test()


if __name__ == "__main__":
    check_settings(SETTINGS)
    start_logging("test_connectivity.log")
    cli()
