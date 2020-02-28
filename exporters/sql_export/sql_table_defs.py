from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine
Base = declarative_base()

class Facet(Base):
    __tablename__ = 'facetter'
    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key= Column(String(250), nullable=False)


class Klasse(Base):
    __tablename__ = 'klasser'

    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key= Column(String(250), nullable=False)
    title = Column(String(250), nullable=False)
    facet_uuid = Column(String, ForeignKey('facetter.uuid'))
    facet_text = Column(String(250), nullable=False)


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
    enhedstype_uuid = Column(String, ForeignKey('klasser.uuid'))
    enhedsniveau_tekst = Column(String(250), nullable=True)
    enhedsniveau_uuid = Column(String, ForeignKey('klasser.uuid'))
    organisatorisk_sti = Column(String(1000), nullable=False)
    # Will be populated before ledere, cannot use ForeignKey
    leder = Column(String(36))
    fungerende_leder = Column(String(36))
    # start_date # TODO


class Adresse(Base):
    __tablename__ = 'adresser'

    uuid = Column(String(36), nullable=False, primary_key=True)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    værdi_text = Column(String(250), nullable=True)
    dar_uuid = Column(String(36), nullable=True)
    adresse_type_text = Column(String(250), nullable=False)
    adresse_type_uuid = Column(String, ForeignKey('klasser.uuid'))
    adresse_type_scope = Column(String(250), nullable=False)
    synlighed_text = Column(String(250), nullable=True)
    synlighed_uuid = Column(String, ForeignKey('klasser.uuid'))
    # start_date # TODO


class Engagement(Base):
    __tablename__ = 'engagementer'

    uuid = Column(String(36), nullable=False, primary_key=True)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    user_key = Column(String(250), nullable=False)
    engagementstype_text = Column(String(250), nullable=False)
    engagementstype_uuid = Column(String, ForeignKey('klasser.uuid'))
    primærtype_text = Column(String(250), nullable=True)
    primærtype_uuid = Column(String, ForeignKey('klasser.uuid'))
    # Workfraction # TODO
    # primærboolean, # TODO
    job_function_text = Column(String(250), nullable=False)
    job_function_uuid = Column(String, ForeignKey('klasser.uuid'))
    # start_date,
    # end_date


class Rolle(Base):
    __tablename__ = 'roller'

    uuid = Column(String(36), nullable=False, primary_key=True)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    role_type_text = Column(String(250), nullable=False)
    role_type_uuid = Column(String, ForeignKey('klasser.uuid'))
    # start_date, # TODO
    # end_date # TODO


class Tilknytning(Base):
    __tablename__ = 'tilknytninger'

    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key = Column(String(250), nullable=False)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String, ForeignKey('enheder.uuid'))
    association_type_text = Column(String(250), nullable=False)
    association_type_uuid = Column(String, ForeignKey('klasser.uuid'))
    # start_date, # TODO
    # end_date # TODO


class Orlov(Base):
    __tablename__ = 'orlover'

    uuid = Column(String(36), nullable=False, primary_key=True)
    user_key = Column(String(250), nullable=False)
    bruger_uuid = Column(String, ForeignKey('brugere.uuid'))
    leave_type_text = Column(String(250), nullable=False)
    leave_type_uuid = Column(String, ForeignKey('klasser.uuid'))
    # start_date # TODO
    # end_date # TODO


class ItSystem(Base):
    __tablename__ = 'it_systemer'

    uuid = Column(String(36), nullable=False, primary_key=True)
    name = Column(String(250), nullable=False)


class ItForbindelse(Base):
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
    manager_type_text = Column(String(250), nullable=False)
    manager_type_uuid = Column(String, ForeignKey('klasser.uuid'))
    niveau_type_text = Column(String(250), nullable=False)
    niveau_type_uuid = Column(String, ForeignKey('klasser.uuid'))


class LederAnsvar(Base):
    __tablename__ = 'leder_ansvar'

    id = Column(Integer, nullable=False, primary_key=True)
    leder_uuid = Column(String, ForeignKey('ledere.uuid'))
    responsibility_text =  Column(String(250), nullable=False)
    responsibility_uuid = Column(String, ForeignKey('klasser.uuid'))
