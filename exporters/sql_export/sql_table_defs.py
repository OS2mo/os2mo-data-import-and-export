from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import DeclarativeMeta

Base: DeclarativeMeta = declarative_base()


class Facet(Base):
    __tablename__ = "wfacetter"
    uuid = Column(String(36), nullable=False, primary_key=True)
    bvn = Column(String(250), nullable=False)


class Klasse(Base):
    __tablename__ = "wklasser"

    uuid = Column(String(36), nullable=False, primary_key=True)
    bvn = Column(String(250), nullable=False)
    titel = Column(String(250), nullable=False)
    facet_uuid = Column(String(36))  # , ForeignKey('facetter.uuid'))
    facet_bvn = Column(String(250), nullable=False)


class Bruger(Base):
    __tablename__ = "wbrugere"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bvn = Column(String(250), nullable=False)
    fornavn = Column(String(250))
    efternavn = Column(String(250))
    kaldenavn_fornavn = Column(String(250))
    kaldenavn_efternavn = Column(String(250))
    cpr = Column(String(250), nullable=True)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class Enhed(Base):
    __tablename__ = "wenheder"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    navn = Column(String(250), nullable=False)
    bvn = Column(String(250), nullable=False)
    forældreenhed_uuid = Column(String(36), primary_key=False)
    enhedstype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    enhedstype_titel = Column(String(250), nullable=False)
    enhedsniveau_uuid = Column(String(36))  # ForeignKey('klasser.uuid'))
    enhedsniveau_titel = Column(String(250))
    organisatorisk_sti = Column(String(1000))
    leder_uuid = Column(String(36))
    fungerende_leder_uuid = Column(String(36))
    startdato = Column(String(10))
    slutdato = Column(String(10))


class Adresse(Base):
    __tablename__ = "wadresser"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    værdi = Column(String(250))
    dar_uuid = Column(String(36))  # , ForeignKey('dar_adresser.uuid'))
    adressetype_uuid = Column(String(36))  # , ForeignKey("klasser.uuid"))
    adressetype_bvn = Column(String(250), nullable=False)
    adressetype_scope = Column(String(250), nullable=False)
    adressetype_titel = Column(String(250), nullable=False)
    synlighed_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    synlighed_scope = Column(String(250))
    synlighed_titel = Column(String(250))
    startdato = Column(String(10))
    slutdato = Column(String(10))


class Engagement(Base):
    __tablename__ = "wengagementer"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    bvn = Column(String(250), nullable=False)
    arbejdstidsfraktion = Column(Integer)
    engagementstype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    engagementstype_titel = Column(String(250), nullable=False)
    primærtype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    primærtype_titel = Column(String(250))
    stillingsbetegnelse_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    stillingsbetegnelse_titel = Column(String(250), nullable=False)
    primær_boolean = Column(Boolean)
    udvidelse_1 = Column(String(250))
    udvidelse_2 = Column(String(250))
    udvidelse_3 = Column(String(250))
    udvidelse_4 = Column(String(250))
    udvidelse_5 = Column(String(250))
    udvidelse_6 = Column(String(250))
    udvidelse_7 = Column(String(250))
    udvidelse_8 = Column(String(250))
    udvidelse_9 = Column(String(250))
    udvidelse_10 = Column(String(250))
    startdato = Column(String(10))
    slutdato = Column(String(10))


class Rolle(Base):
    __tablename__ = "wroller"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    rolletype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    rolletype_titel = Column(String(250), nullable=False)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class Tilknytning(Base):
    __tablename__ = "wtilknytninger"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bvn = Column(String(250), nullable=False)
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    tilknytningstype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    tilknytningstype_titel = Column(String(250))
    stillingsbetegnelse_uuid = Column(String(36))
    stillingsbetegnelse_titel = Column(String(250))
    it_forbindelse_uuid = Column(String(36))
    startdato = Column(String(10))
    slutdato = Column(String(10))
    primær_boolean = Column(Boolean, nullable=True)


class Orlov(Base):
    __tablename__ = "worlover"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bvn = Column(String(250), nullable=False)
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    orlovstype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    orlovstype_titel = Column(String(250), nullable=False)
    engagement_uuid = Column(String(36))
    startdato = Column(String(10))
    slutdato = Column(String(10))


class ItSystem(Base):
    __tablename__ = "wit_systemer"

    uuid = Column(String(36), nullable=False, primary_key=True)
    navn = Column(String(250), nullable=False)


class ItForbindelse(Base):
    __tablename__ = "wit_forbindelser"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    it_system_uuid = Column(String(36))  # , ForeignKey('it_systemer.uuid'))
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    brugernavn = Column(String(250))
    primær_boolean = Column(Boolean, nullable=False)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class Leder(Base):
    __tablename__ = "wledere"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    ledertype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    ledertype_titel = Column(String(250), nullable=False)
    niveautype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    niveautype_titel = Column(String(250), nullable=False)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class LederAnsvar(Base):
    __tablename__ = "wleder_ansvar"

    id = Column(Integer, nullable=False, primary_key=True)
    leder_uuid = Column(String(36))  # , ForeignKey('ledere.uuid'))
    lederansvar_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    lederansvar_titel = Column(String(250), nullable=False)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class KLE(Base):
    __tablename__ = "wkle"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    kle_aspekt_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    kle_aspekt_titel = Column(String(250), nullable=False)
    kle_nummer_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    kle_nummer_titel = Column(String(250), nullable=False)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class Kvittering(Base):
    __tablename__ = "kvittering"

    id = Column(Integer, nullable=False, primary_key=True)
    query_tid = Column(DateTime)
    start_levering_tid = Column(DateTime)
    slut_levering_tid = Column(DateTime)


class Enhedssammenkobling(Base):
    __tablename__ = "wenhedssammenkobling"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    enhed1_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    enhed2_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    startdato = Column(String(10))
    slutdato = Column(String(10))


class DARAdresse(Base):
    __tablename__ = "wdar_adresser"

    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    vejkode = Column(String(8))
    vejnavn = Column(String(250))
    husnr = Column(String(8))
    etage = Column(String(16))
    dør = Column(String(8))
    postnr = Column(String(8))
    postnrnavn = Column(String(250))
    kommunekode = Column(String(8))
    adgangsadresseid = Column(String(36))
    betegnelse = Column(String(250))
