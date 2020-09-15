from deepdiff import DeepDiff
from pprint import pprint

from user_names import CreateUserNames
from utils import AttrDict, recursive_dict_update

from exporters.sql_export.lora_cache import LoraCache
from integrations.ad_integration.ad_writer import LoraCacheSource, MORESTSource
from integrations.ad_integration.ad_writer import ADWriter
from integrations.ad_integration.ad_exceptions import ManagerNotUniqueFromCprException


class ADWriterX(ADWriter):
    def _init_name_creator(self):
        """Mocked to pretend no names are occupied.

        This method would normally use ADReader to read usernames from AD.
        """
        # Simply leave out the call to populate_occupied_names
        self.name_creator = CreateUserNames(occupied_names=set())

    def _create_session(self):
        """Mocked to return a fake-class which writes scripts to self.scripts.

        This method would normally send scripts to powershell via WinRM.
        """

        def run_ps(ps_script):
            # Fake the WinRM run_ps return type
            return AttrDict(
                {
                    "status_code": 0,
                    "std_out": b"",
                    "std_err": b"",
                }
            )

        # Fake the WinRM session object
        return AttrDict(
            {
                "run_ps": run_ps,
            }
        )

    def _get_retry_exceptions(self):
        """Mocked to return an empty list, i.e. never retry.

        This method would normally return the WinRM transport exception, to
        cause retrying to happen.
        """
        return []


def return_exception(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except TypeError as exp:
        print(exp)
        return exp
    except Exception as exp:
        return exp


class SkipUser(Exception):
    pass


def equivalence_generator(lc_variant, mo_variant, users):
    def test_equivalence(method_name, uuid_transformer=None):
        print("Testing equivalence (" + method_name + ")")
        uuid_transformer = uuid_transformer or (lambda uuid: [uuid])
        differences = 0
        skipped = 0
        transformer_errors = 0
        for user_uuid in users:
            try:
                transformed = uuid_transformer(user_uuid)
            except SkipUser:
                skipped += 1
                continue
            except Exception as exp:
                print(type(exp), exp)
                transformer_errors += 1
                continue
            mo_value = return_exception(getattr(mo_variant, method_name), *transformed)
            lc_value = return_exception(getattr(lc_variant, method_name), *transformed)
            difference = DeepDiff(mo_value, lc_value)
            if difference:
                pprint(difference, indent=2)
                differences += 1
        total = len(users)
        print(total - differences - skipped - transformer_errors, "ok")
        print(differences, "differences")
        print(skipped, "skipped")
        print(transformer_errors, "transformer errors")
        print(total, "total")
        print()
        return differences
    return test_equivalence


def main():
    print("Fetch all user uuids")
    from os2mo_helpers import mora_helpers
    from operator import itemgetter
    morahelper = mora_helpers.MoraHelper("http://localhost:5000")
    users = list(map(itemgetter('uuid'), morahelper.read_all_users()))
    print(len(users), "users")
    print()

    print("Populating LoraCache")
    lc = LoraCache(resolve_dar=False, full_history=False)
    lc.populate_cache(dry_run=False, skip_associations=True)
    print("Calculating LoraCache values")
    lc.calculate_derived_unit_data()
    lc.calculate_primary_engagements()
    print()

    print("Populating historic LoraCache")
    lc_historic = LoraCache(resolve_dar=False, full_history=True,
                            skip_past=False)
    lc_historic.populate_cache(dry_run=False, skip_associations=True)
    print()

    def datasource_equivalence():
        print("Datasource equivalence testing")
        mrs = MORESTSource({'global': {'mora.base': 'http://localhost:5000'}})
        lcs = LoraCacheSource(lc, lc_historic, mrs)
        ds_equivalence = equivalence_generator(lcs, mrs, users)

        #ds_equivalence("read_user")
        #ds_equivalence("get_email_address")
        #ds_equivalence("find_primary_engagement")

        # XXX: NOT EQUIVALENT
        from integrations.ad_integration.ad_exceptions import NoActiveEngagementsException
        def uuid_to_args(uuid):
            mo_user = lcs.read_user(uuid)
            try:
                _, _, eng_org_unit, eng_uuid = lcs.find_primary_engagement(uuid)
            except NoActiveEngagementsException:
                raise SkipUser
            return mo_user, eng_org_unit, eng_uuid
        ds_equivalence("get_manager_uuid", uuid_to_args)

    def adwriter_equivalence():
        print("ADWriter equivalence testing")
        settings = {
            "integrations.ad.winrm_host": "dummy",
            "integrations.ad.search_base": "search_base",
            "integrations.ad.cpr_field": "cpr_field",
            "integrations.ad.cpr_seperator": "cpr_sep",
            # "integrations.ad.sam_filter": "sam_filter",
            "integrations.ad.system_user": "system_user",
            "integrations.ad.password": "password",
            "integrations.ad.properties": "properties",
            "mora.base": "http://localhost:5000",
            "integrations.ad.write.uuid_field": "uuid_field",
            "integrations.ad.write.level2orgunit_field": "level2orgunit_field",
            "integrations.ad.write.org_unit_field": "org_field",
            "integrations.ad.write.upn_end": "epn_end",
            "integrations.ad.write.org_unit_field": "org_field",
            "integrations.ad.write.level2orgunit_type": "level2orgunit_type",
            "integrations.ad.cpr_field": "cpr_field",
            "integrations.ad.cpr_separator": "ad_cpr_sep",
            "integrations.ad.ad_mo_sync_mapping": {},
            "address.visibility.public": "address_visibility_public_uuid",
            "address.visibility.internal": "address_visibility_internal_uuid",
            "address.visibility.secret": "address_visibility_secret_uuid",
        }
        from integrations.ad_integration.read_ad_conf_settings import read_settings
        settings = read_settings(settings)

        lc_writer = ADWriterX(lc=lc, lc_historic=lc_historic, all_settings=settings)
        mrs_writer = ADWriterX(all_settings=settings)
        aw_equivalence = equivalence_generator(lc_writer, mrs_writer, users)

        aw_equivalence("read_ad_information_from_mo")

    datasource_equivalence()
    # adwriter_equivalence()


if __name__ == '__main__':
    main()
