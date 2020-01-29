import json
import pathlib
import argparse
import requests
import sd_payloads
from os2mo_helpers.mora_helpers import MoraHelper
from integrations.SD_Lon.sd_common import sd_lookup
from integrations.SD_Lon.sd_common import mora_assert


class JobIdSync(object):
    def __init__(self):
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        helper = MoraHelper(hostname=self.settings['mora.base'],
                            use_cache=False)
        self.engagement_types = helper.read_classes_in_facet('engagement_type')

    def _find_engagement_type(self, job_pos_id):
        """
        Find the Klasse corresponding to job_pos_id in LoRa.
        """
        found_type = None
        for engagement_type in self.engagement_types[0]:
            if engagement_type['user_key'] == str(job_pos_id):
                found_type = engagement_type
        return found_type

    def _edit_engagement_type(self, uuid, title):
        """
        Change the title of an existing LoRa engagement type.
        """
        payload = sd_payloads.edit_engagement_type(title)
        response = requests.patch(
            url=self.settings['mox.base'] + '/klassifikation/klasse/' + uuid,
            json=payload
        )
        mora_assert(response)
        return response

    def get_job_pos_id_from_sd(self, job_pos_id):
        """
        Return the textual value of a Job Position Identifier fro SD.
        """
        params = {
            'JobPositionIdentifier': job_pos_id,
        }
        job_pos_response = sd_lookup('GetProfession20080201', params)
        if 'Profession' in job_pos_response:
            job_pos = job_pos_response['Profession']['JobPositionName']
        else:
            job_pos = None
        return job_pos

    def sync_to_sd(self, job_pos_id):
        """
        Sync the titel of LoRa engagement type to the value current
        registred at SD.
        """
        job_pos = self.get_job_pos_id_from_sd(job_pos_id)
        if job_pos is None:
            return 'Job position not found i SD'

        mo_type = self._find_engagement_type(job_pos_id)
        if mo_type is None:
            return 'Job position not found i MO'

        self._edit_engagement_type(mo_type['uuid'], job_pos)
        return 'Job position updated'

    def sync_manually(self, job_pos_id, titel):
        """
        Manually update the titel of an engagement type.
        """
        mo_type = self._find_engagement_type(job_pos_id)
        if mo_type is None:
            return 'Job position not found i MO'

        self._edit_engagement_type(mo_type['uuid'], titel)
        return 'Job position updated'

    def _cli(self):
        """
        Command line interface for the Job Position Sync tool.
        If only job_pos_id is given, value will be extracted from SD.
        If a title is also given, the titel will be synced independant of
        the SD value.
        """
        parser = argparse.ArgumentParser(description='JobIdentifier Sync')
        parser.add_argument('--job-pos-id', nargs=1, required=True,
                            metavar='SD_Job_position_ID')
        parser.add_argument('--titel', nargs=1, required=False, metavar='Titel')
        args = vars(parser.parse_args())

        job_pos_id = args.get('job_pos_id')[0]
        print(job_pos_id)
        title = args.get('titel')
        if title is None:
            print(self.sync_to_sd(job_pos_id))
        else:
            print(self.sync_manually(job_pos_id, title[0]))


if __name__ == '__main__':
    sync_tool = JobIdSync()
    sync_tool._cli()
