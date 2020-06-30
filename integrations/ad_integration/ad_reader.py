import time
import random
import logging
from winrm import Session

from integrations.ad_integration.ad_common import AD
from integrations.ad_integration import read_ad_conf_settings

logger = logging.getLogger("AdReader")


# SKIP_BRUGERTYPE


class ADParameterReader(AD):

    def read_encoding(self):
        """
        Read the character encoding of the Power Shell session.
        """
        ps_script = "$OutputEncoding | ConvertTo-Json"
        response = self._run_ps_script(ps_script)
        return response

    def read_it_all(self):
        settings = self._get_setting()
        response = self.cache_all()
        users = [
            user
            for user in response
            if self.is_included(settings, user)
        ]
        return users


    def is_included(self, settings, user):
        """ include/exclude users depending on settings
        in order to (#36182) identify primary ad user so
        we only return one user per cpr number
        """
        discrim_field = settings["discriminator.field"]
        if discrim_field is not None and discrim_field in user:
            value_found = False
            for i in settings["discriminator.values"]:
                value_found = i in user[discrim_field]
                if value_found:
                    break
            if settings["discriminator.function"] == "include":
                return value_found
            elif settings["discriminator.function"] == "exclude":
                return not value_found

        return True

    # Hvornår skal vi læse og skrive i hvad?
    # Opdatering af os2mo fra ad: ad_sync: vi læser alle (for i in...) Ny reader og opdater mo
    # Opdatering af ad fra mo (mo_to_ad_sync) skriv kun til det første (og læs kun fra det første)
    # Indlæsning fra Opus og SD: Her læser vi fra 0'eren - vi skal have fat i object-guid
    # cpr-uid-map-til rollekatalog skal også kun læse fra 0 (for now....)
    # vi kan teste en del med den, altså: venv/bin/python exporters/cpr_uuid.py --use-ad
    # cpr_mo_ad_map.csv har kun uuider/brugernavne pǻ de linier, hvor den sd-importerede bruger også er i ad.


    def uncached_read_user(self, user=None, cpr=None, ria=None):
        # Bug, currently this will not work directly with the school domain. Users
        # will be cached (and can be read by read_user) but will not be returned
        # directly by this function
        logger.debug('Uncached AD read, user {}, cpr {}'.format(user, cpr))

        server = random.choice(self.all_settings['primary']['servers'])
        response = self.get_from_ad(user=user, cpr=cpr, server=server)
        current_user = {}
        try:
            for current_user in response:
                settings = self._get_setting()

                # Viborg special case
                job_title = current_user.get('Title')
                if job_title and job_title.find('FRATR') == 0:
                    continue  # These are users that has left

                # Viborg - move this to settings
                #brugertype = current_user.get('xBrugertype')
                #if brugertype and brugertype.find('Medarbejder') == -1:
                #    continue

                if not self.is_included(settings, current_user):
                    continue

                # This will result in an error further down
                # but is left in order to have comparable errors
                # to previous runs
                if not current_user:
                    current_user = {}

                cpr = current_user[settings['cpr_field']].replace(
                    settings['cpr_separator'], '')

                self.results[current_user['SamAccountName']] = current_user


                if current_user['SamAccountName'].startswith(settings['sam_filter']):
                    self.results[cpr] = current_user

            if ria:
                ria.extend(response)

            return current_user
        except Exception:
            logger.error('Response from uncached_read_user: {}'.format(response))
            raise


    def cache_all(self):
        logger.info('Caching all users')
        t = time.time()
        return_value=[]
        for i in range(1, 32):
            day = str(i).zfill(2)
            self.uncached_read_user(cpr='{}*'.format(day), ria=return_value)
            logger.debug(len(self.results))
            logger.debug('Read time: {}'.format(time.time() - t))
        return return_value

    def read_user(self, user=None, cpr=None, cache_only=False):
        """
        Read all properties of an AD user. The user can be retrived either by cpr
        or by AD user name.
        :param user: The AD username to retrive.
        :param cpr: cpr number of the user to retrive.
        :return: All properties listed in AD for the user.
        """
        logger.debug('Cached AD read, user {}, cpr {}'.format(user, cpr))
        if (not cpr) and (not user):
            return

        if user:
            dict_key = user
            if user in self.results:
                return self.results[user]

        if cpr:
            dict_key = cpr
            if cpr in self.results:
                return self.results[cpr]

        if cache_only:
            return {}

        # Poulate self.results:
        self.uncached_read_user(user=user, cpr=cpr)

        logger.debug('Returned info for {}: {}'.format(
            dict_key, self.results.get(dict_key, {})))
        return self.results.get(dict_key, {})


if __name__ == '__main__':
    ad_reader = ADParameterReader()
    #import pickle
    #with open("mypickle.p","bw") as f:
    #    f.write(pickle.dumps(ad_reader.read_it_all()))
    everything = ad_reader.read_it_all()
    for user in everything:
        print('Name: {}, Sam: {}, Manager: {} CPR: {}'.format(
            user['Name'], user['SamAccountName'], user.get('Manager'), "cpr: " + str(user.get('xAttrCPR'))))
        if user['SamAccountName'] == 'johndoe':
            for key in sorted(user.keys()):
                print('{}: {}'.format(key, user[key]))
