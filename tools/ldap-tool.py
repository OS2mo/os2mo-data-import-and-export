import ldap

from ra_utils.load_settings import load_settings

settings = load_settings()
this_ad =settings['integrations.ad'][0]
url = this_ad['servers'][0]
usr = this_ad['system_user']
passwd = this_ad['password']
search_base = this_ad['search_base']
connect = ldap.initialize(f'ldap://{url}')

connect.set_option(ldap.OPT_REFERRALS, 0)
connect.simple_bind_s(usr, passwd)

result = connect.search_s(search_base,
                          ldap.SCOPE_SUBTREE)
print(result)