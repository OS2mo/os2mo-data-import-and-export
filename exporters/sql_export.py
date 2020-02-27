import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine

from exporters.lora_cache import LoraCache

Base = declarative_base()

class Facet(Base):
    __tablename__ = 'facetter'
    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key= Column(String(250), nullable=False)
    title = Column(String(250), nullable=False)

class Klasse(Base):
    __tablename__ = 'klasser'

    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key= Column(String(250), nullable=False)
    title = Column(String(250), nullable=False)
    facet_uuid = Column(String, ForeignKey('facetter.uuid'))

class Bruger(Base):
    __tablename__ = 'brugere'

    uuid = Column(String(36), nullable=False, primary_key=True)
    fornavn = Column(String(250), nullable=True)
    efternavn = Column(String(250), nullable=True)
    cpr = Column(String(250), nullable=False)


class Enhed(Base):
    __tablename__ = 'enheder'

    uuid = Column(String(36), nullable=False, primary_key=True)
    navn = Column(String(250), nullable=False)
    # Undersøg, dette er en refence til tabellen selv
    forældreenhed_uuid = Column(String(36), nullable=True, primary_key=False)
    enhedstype_text = Column(String(250), nullable=False)
    enhedstype_uuid = Column(String(36), nullable=False)
    enhedsniveau_tekst = Column(String(250), nullable=True)
    enhedsniveau_uuid = Column(String(36), nullable=True)
    organisatorisk_sti = Column(String(1000), nullable=False)
    # start_date # TODO


class Adresse(Base):
    __tablename__ = 'adresser'

    uuid = Column(String(36), nullable=False, primary_key=True)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    værdi_text = Column(String(250), nullable=True)
    dar_uuid = Column(String(36), nullable=True)
    adresse_type_text = Column(String(250), nullable=False)
    adresse_type_uuid = Column(String(36), nullable=False)
    adresse_type_scope = Column(String(250), nullable=False)
    synlighed_text = Column(String(250), nullable=True)
    synlighed_uuid = Column(String(36), nullable=True)
    # start_date # TODO


class Engagement(Base):
    __tablename__ = 'engagementer'

    uuid = Column(String(36), nullable=False, primary_key=True)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    user_key = Column(String(250), nullable=False)
    engagementstype_text = Column(String(250), nullable=False)
    engagementstype_uuid = Column(String(36), nullable=False)
    primærtype_text = Column(String(250), nullable=True)
    primærtype_uuid = Column(String(36), nullable=True)
    # Workfraction # TODO
    # primærboolean, # TODO
    job_function_text = Column(String(250), nullable=False)
    job_function_uuid = Column(String(36), nullable=False)
    # start_date,
    # end_date


class Rolle(Base):
    __tablename__ = 'roller'

    uuid = Column(String(36), nullable=False, primary_key=True)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    role_type_text = Column(String(250), nullable=False)
    role_type_uuid = Column(String(36), nullable=False)
    # start_date, # TODO
    # end_date # TODO


class Tilknytning(Base):
    __tablename__ = 'tilknytninger'

    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key = Column(String(250), nullable=False)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    association_type_text = Column(String(250), nullable=False)
    association_type_uuid = Column(String(36), nullable=False)
    # start_date, # TODO
    # end_date # TODO


class Orlov(Base):
    __tablename__ = 'orlover'

    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key = Column(String(250), nullable=False)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    leave_type_text = Column(String(250), nullable=False)
    leave_type_uuid = Column(String(36), nullable=False)
    # start_date # TODO
    # end_date # TODO


class IT_system(Base):
    __tablename__ = 'it_systemer'

    uuid = Column(String(36), nullable=False, primary_key=True)
    name = Column(String(250), nullable=False)


class IT_forbindelse(Base):
    __tablename__ = 'it_forbindelser'

    uuid = Column(String(36), nullable=False, primary_key=True)
    it_system_uuid = Column(String, ForeignKey('it_systemer.uuid'))
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    brugernavn = Column(String(250), nullable=True)


class Leder(Base):
    __tablename__ = 'ledere'

    uuid = Column(String(36), nullable=False, primary_key=True)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    # nedarvet (True / False)  # TODO
    manager_type_text = Column(String(250), nullable=False)
    manager_type_uuid = Column(String(36), nullable=False)
    niveau_type_text = Column(String(250), nullable=False)
    niveau_type_uuid = Column(String(36), nullable=False)

# class Leder_ansvar(Base):
#     __tablename__ = 'leder_ansvar'

#     leder_uuid = Column(String, ForeignKey('ledere.uuid'))
#     responsibility_text =  Column(String(250), nullable=False)
#     responsibility_uuid = Column(String(36), nullable=False)


engine = create_engine('sqlite:///mo.db')

Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()


lc = LoraCache()
lc.populate_cache()


# class Facet(Base):
#     __tablename__ = 'facetter'
#     uuid = Column(String(36), nullable=False, primary_key=True)
#     user_key= Column(String(250), nullable=False)
#     title = Column(String(250), nullable=False)

# class Klasse(Base):
#     __tablename__ = 'klasser'

#     uuid = Column(String(36), nullable=False, primary_key=True)
#     user_key= Column(String(250), nullable=False)
#     title = Column(String(250), nullable=False)
#     facet_uuid = Column(String, ForeignKey('facetter.uuid'))



for user, user_info in lc.users.items():
    sql_user = Bruger(
        uuid=user,
        fornavn=user_info['fornavn'],
        efternavn=user_info['efternavn'],
        cpr=user_info['cpr']
    )
    session.add(sql_user)
session.commit()


for unit, unit_info in lc.units.items():
    sql_unit = Enhed(
        uuid=unit,
        navn=unit_info['name'],
        forældreenhed_uuid=unit_info['parent'],
        enhedstype_text=lc.classes[unit_info['unit_type']]['title'],
        enhedstype_uuid=unit_info['unit_type'],
        enhedsniveau_tekst=lc.classes[unit_info['level']]['title'],
        enhedsniveau_uuid=unit_info['level'],
        organisatorisk_sti=''  # TODO
    )
    session.add(sql_unit)
session.commit()


for address, address_info in lc.addresses.items():
    synlighed_text = None
    if address_info['visibility'] is not None:
        synlighed_text = lc.classes[address_info['visibility']]['title']

    sql_address = Adresse(
        uuid=address,
        bruger_uuid=address_info['user'],
        enhed_uuid=address_info['unit'],
        værdi_text=address_info['value'],
        dar_uuid=address_info['dar_uuid'],
        adresse_type_text=lc.classes[address_info['adresse_type']]['title'],
        adresse_type_uuid=address_info['adresse_type'],
        adresse_type_scope=address_info['scope'],
        synlighed_uuid=address_info['visibility'],
        synlighed_text=synlighed_text
    )
    session.add(sql_address)
session.commit()


for engagement, engagement_info in lc.engagements.items():
    sql_engagement = Engagement(
        uuid=engagement,
        bruger_uuid=engagement_info['user'],
        enhed_uuid=engagement_info['unit'],
        user_key=engagement_info['user_key'],
        engagementstype_text=lc.classes[engagement_info['engagement_type']]['title'],
        engagementstype_uuid=engagement_info['engagement_type'],
        primærtype_text=lc.classes[engagement_info['primary_type']]['title'],
        primærtype_uuid=engagement_info['primary_type'],
        # primærboolean, # TODO
        job_function_text=lc.classes[engagement_info['job_function']]['title'],
        job_function_uuid=engagement_info['job_function']
    )
    session.add(sql_engagement)
session.commit()


for association, association_info in lc.associations.items():
    sql_association = Tilknytning(
        uuid=association,
        bruger_uuid=association_info['user'],
        enhed_uuid=association_info['unit'],
        user_key=association_info['user_key'],
        association_type_text=lc.classes[association_info['association_type']]['title'],
        association_type_uuid=association_info['association_type']
    )
    session.add(sql_association)
session.commit()

for role, role_info in lc.roles.items():
    sql_role = Rolle(
        uuid=role,
        bruger_uuid=role_info['user'],
        enhed_uuid=role_info['unit'],
        role_type_text=lc.classes[role_info['role_type']]['title'],
        role_type_uuid=role_info['role_type']
        # start_date, # TODO
        # end_date # TODO
    )
    session.add(sql_role)
session.commit()



for leave, leave_info in lc.leaves.items():
    sql_leave = Orlov(
        uuid=leave,
        user_key=leave_info['user_key'],
        bruger_uuid=leave_info['user'],
        leave_type_text = lc.classes[leave_info['leave_type']]['title'],
        leave_type_uuid = leave_info['leave_type'],
        # start_date # TODO
        # end_date # TODO
    )
    session.add(sql_leave)
session.commit()

for manager, manager_info in lc.managers.items():
    sql_manager = Leder(
        uuid=manager,
        bruger_uuid=manager_info['user'],
        enhed_uuid=manager_info['unit'],
        # nedarvet (True / False)  # TODO
        manager_type_text=lc.classes[manager_info['manager_type']]['title'],
        manager_type_uuid=manager_info['manager_type'],
        niveau_type_text=lc.classes[manager_info['manager_level']]['title'],
        niveau_type_uuid=manager_info['manager_level']
    )
    session.add(sql_manager)
session.commit()


for itsystem, itsystem_info in lc.itsystems.items():
    sql_itsystem = IT_system(
        uuid = itsystem,
        name = itsystem_info['name']
    )
    session.add(sql_itsystem)
session.commit()

for it_connection, it_connection_info in lc.it_connections.items():
    sql_it_connection = IT_forbindelse(
        uuid=it_connection,
        it_system_uuid=it_connection_info['itsystem'],
        bruger_uuid=it_connection_info['user'],
        enhed_uuid=it_connection_info['unit'],
        brugernavn=it_connection_info['username']
    )
    session.add(sql_it_connection)
session.commit()


# for result in engine.execute('select * from enheder'):
#    print(result)

# for result in engine.execute('select * from brugere'):
# print(result)

# for result in engine.execute('select * from adresser limit 20'):
#     print()
#     print(result.items())

# for result in engine.execute('select * from engagementer limit 20'):
#     print()
#     print(result.items())

# for result in engine.execute('select * from ledere limit 20'):
#     print()
#     print(result.items())

# for result in engine.execute('select * from tilknytninger limit 20'):
#    print()
#    print(result.items())

#for result in engine.execute('select * from orlover limit 20'):
#    print()
#    print(result.items())

# for result in engine.execute('select * from it_systemer limit 20'):
#     print()
#     print(result.items())

# for result in engine.execute('select * from it_forbindelser limit 20'):
#     print()
#     print(result.items())

for result in engine.execute('select * from roller limit 20'):
    print()
    print(result.items())
