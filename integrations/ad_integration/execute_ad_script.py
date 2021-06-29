import logging
from pathlib import Path

import click
from click_option_group import optgroup
from click_option_group import RequiredMutuallyExclusiveOptionGroup

from .ad_exceptions import NoScriptToExecuteException
from .ad_exceptions import UnknownKeywordsInScriptException
from .ad_writer import ADWriter


logger = logging.getLogger("AdExecute")


EXECUTION_REPLACEMENT_SCAFFOLD = {
    "%OS2MO_AD_BRUGERNAVN%": None,
    "%OS2MO_BRUGER_FORNAVN%": None,
    "%OS2MO_BRUGER_EFTERNAVN%": None,
    "%OS2MO_BRUGER_CPR%": None,
    "%OS2MO_LEDER_EMAIL%": None,
    "%OS2MO_LEDER_NAVN%": None,
    "%OS2MO_BRUGER_ENHED%": None,
    "%OS2MO_BRUGER_ENHED_UUID%": None,
}


class ADExecute(ADWriter):
    def __init__(self):
        super().__init__()
        self.script = None

    def _remove_block_comments(self):
        search_index = 0
        index = self.script.find("<#", search_index)
        remove_ranges = []
        while index > 0:
            end_index = self.script.find("#>", index + 1) + 3
            remove_ranges.append((index, end_index))
            search_index = end_index
            index = self.script.find("<%", search_index)

        for remove_range in remove_ranges:
            self.script = (
                self.script[: remove_range[0]] + self.script[remove_range[1] :]
            )

    def validate_script(self, pre_check=False):
        """
        Validate that the currently loaded script is valid for execution,
        to be valid it must exist, it cannot contain unknown keywords.
        :param pre_check: If true, the errors will be reported back as
        a list, rather than as exceptions.
        """
        if self.script is None:
            msg = "No script loaded!"
            if pre_check:
                return (False, msg)
            logger.error(msg)
            raise NoScriptToExecuteException(msg)

        lines = self.script.split("\n")

        keywords = set()
        for line in lines:
            if line.find("#") > -1:
                line = line[0 : line.find("#")]
            search_index = 0
            index = line.find("%OS2MO_", search_index)
            while index > 0:
                end_index = line.find("%", index + 1)
                keywords.add(line[index : end_index + 1])
                search_index = end_index
                index = line.find("%OS2MO_", search_index)

        keyword_errors = set()
        for keyword in keywords:
            if keyword not in EXECUTION_REPLACEMENT_SCAFFOLD:
                keyword_errors.add(keyword)
        if keyword_errors:
            msg = "Unknown keywords present in template: {}".format(keyword_errors)
            if pre_check:
                return (False, msg)
            logger.error(msg)
            raise UnknownKeywordsInScriptException(msg)
        return (True, "")

    def fill_script_template(self, mo_user_uuid):
        mo_info = self.read_ad_information_from_mo(mo_user_uuid, read_manager=True)
        ad_user = self.get_from_ad(cpr=mo_info["cpr"])
        user_sam = ad_user[0]["SamAccountName"]

        execution_replacement = EXECUTION_REPLACEMENT_SCAFFOLD.copy()
        execution_replacement["%OS2MO_AD_BRUGERNAVN%"] = user_sam
        execution_replacement["%OS2MO_BRUGER_FORNAVN%"] = mo_info["name"][0]
        execution_replacement["%OS2MO_BRUGER_EFTERNAVN%"] = mo_info["name"][1]
        execution_replacement["%OS2MO_BRUGER_CPR%"] = mo_info["cpr"]
        execution_replacement["%OS2MO_LEDER_EMAIL%"] = mo_info["manager_mail"]
        execution_replacement["%OS2MO_LEDER_NAVN%"] = mo_info["manager_name"]
        execution_replacement["%OS2MO_BRUGER_ENHED%"] = mo_info["unit"]
        execution_replacement["%OS2MO_BRUGER_ENHED_UUID%"] = mo_info["unit_uuid"]
        if None in execution_replacement:
            msg = "Not all replacement values available"
            logger.error(msg)
            # TODO: Should we make an explicit exception for this?
            raise Exception(msg)

        actual_script = ""
        lines = self.script.split("\n")
        for line in lines:
            if line.find("#") > -1:
                actual_line = line[0 : line.find("#")]
            else:
                actual_line = line

            for key, replacement in execution_replacement.items():
                actual_line = actual_line.replace(key, replacement)
            if actual_line.strip():
                actual_script += actual_line + "\n"

        self.script = actual_script
        return True

    def read_script_template(self, script_name, pre_check=False):
        if not script_name.endswith("ps_template"):
            script_name += ".ps_template"
        p = Path("scripts/{}".format(script_name))

        if p.is_file():
            self.script = p.read_text()
        validation = self.validate_script(pre_check)
        return validation

    def execute_script(self, script, user_uuid):
        self.read_script_template(script)

        # Remove block comments before performing the validation.
        self._remove_block_comments()

        success = self.fill_script_template(user_uuid)
        if not success:
            msg = "Failed to fill in template"
            logger.error(msg)
            raise Exception(msg)

        response = self._run_ps_script(script)
        if not response == {}:
            msg = "Failed to execute this: {}".format(self.script)
            logger.error(msg)
            msg = "Power Shell error: {}".format(response)
            logger.error(msg)
            raise Exception(msg)
        return "Script completed"


@click.command(help="Powershell Script Executer")
@optgroup.group("Action", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option(
    "--validate-script",
    help="Validate that a template can be parsed",
)
@optgroup.option(
    "--execute-script",
    help="Execute script with values from user",
    nargs=2,
    type=str,
)
def cli(**args):
    """
    Command line interface for the script executor.
    """
    executor = ADExecute()
    if args.get("validate_script"):
        script = args["validate_script"]
        valid = executor.read_script_template(script, pre_check=True)
        if valid[0]:
            print("Script is valid")
        else:
            print("Script validation failed:\n{}".format(valid[1]))

    if args.get("execute_script"):
        script, user = args["execute_script"]
        print(executor.execute_script(script, user))


if __name__ == "__main__":
    cli()
    # This is a fictious user, Noah Petersen, 111111-1111
    # mo_user = '4931ddb6-5084-45d6-9fb2-52ff33998005'
    # script = 'send_email.ps_template'
    # exe.execute_script(script, mo_user)
