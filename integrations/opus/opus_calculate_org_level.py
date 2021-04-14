from os2mo_helpers.mora_helpers import MoraHelper
from operator import itemgetter
import constants
from exporters.utils.load_settings import load_settings

from integrations.opus import opus_helpers, payloads



settings = load_settings()
helper = MoraHelper(hostname=settings.get('mora.base'))
org= helper.read_organisation()
organisation = helper._mo_lookup(org, 'o/{}/ou/')['items']
centres = list(filter(lambda org: 'test' in org['name'], organisation ))
organisation = helper.read_ou_tree('eadd857c-8b3b-8efb-f650-78629dc9f39c')

print(organisation)