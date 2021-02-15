from tqdm import tqdm
from os2mo_helpers.mora_helpers import MoraHelper
import asyncio
from aiohttp import ClientSession

class Morahelper_async(MoraHelper):

    async def read_organisation(self, session):
        """Read the main Organisation, all OU's will have this as root.

        Currently reads only one, theroretically more than root org can exist.
        :return: UUID of root organisation
        """
        org_id = await self._mo_lookup(session, uuid=None, url='o/')
        return org_id[0]['uuid']

    async def _mo_lookup(self, session, uuid, url, at=None, validity=None, only_primary=False,
                   calculate_primary=False, SAML_TOKEN=None):

            params = {}
            if calculate_primary:
                params['calculate_primary'] = 1
            if only_primary:
                params['only_primary_uuid'] = 1
            if at:
                params['at'] = at
            elif validity:
                params['validity'] = validity

            full_url = self.host + url.format(uuid)

            async with session.get(full_url, params=params) as response:
                return await response.json()
                

async def main():
    helper = Morahelper_async()
    async with ClientSession() as session:
        org_uuid = await helper.read_organisation(session)
        units = await helper._mo_lookup(session, uuid=org_uuid, url='/o/{}/ou/')
        tasks =  []
        for u in tqdm(units['items']):
            task = asyncio.ensure_future(helper._mo_lookup(session, uuid=u['uuid'], url='ou/{}'))
            tasks.append(task)            
        responses = await asyncio.gather(*tasks)

def read_all_organisations():
    helper = MoraHelper()
    org_uuid = helper.read_organisation()
    units = helper._mo_lookup(org_uuid, '/o/{}/ou/')
    return [helper.read_ou(u['uuid']) for u in tqdm(units['items'])]

import time
s = time.perf_counter()
asyncio.run(main())
elapsed = time.perf_counter() - s
print(f"Async executed in {elapsed:0.2f} seconds.")




s = time.perf_counter()
read_all_organisations()
elapsed = time.perf_counter() - s
print(f"Sync executed in {elapsed:0.2f} seconds.")