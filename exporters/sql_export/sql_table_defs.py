from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.ext.declarative import declarative_base

Base: DeclarativeMeta = declarative_base()  # type: ignore


class Compare:
    def __eq__(self, __value: object) -> bool:
        """When comparing objects we need to disregard the fields:

        * ID (as it is auto generated by sql-alchemy)
        * _sa_instance_state
        """

        left = dict(self.__dict__)
        left.pop("id", None)
        left.pop("_sa_instance_state", None)
        right = dict(__value.__dict__)
        right.pop("id", None)
        right.pop("_sa_instance_state", None)
        return left == right


class BaseFacet(Compare):
    bvn = Column(String(250), nullable=False)
    uuid = Column(String(36), nullable=False, primary_key=True)


class WFacet(Base, BaseFacet):  # type: ignore
    __tablename__ = "wfacetter"


class Facet(Base, BaseFacet):  # type: ignore
    __tablename__ = "facetter"


class BaseKlasse(Compare):
    uuid = Column(String(36), nullable=False, primary_key=True)
    bvn = Column(String(250), nullable=False)
    titel = Column(String(250), nullable=False)
    facet_uuid = Column(String(36))  # , ForeignKey('facetter.uuid'))
    facet_bvn = Column(String(250), nullable=False)


class WKlasse(Base, BaseKlasse):  # type: ignore
    __tablename__ = "wklasser"


class Klasse(Base, BaseKlasse):  # type: ignore
    __tablename__ = "klasser"


class BaseBruger(Compare):
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


class WBruger(Base, BaseBruger):  # type: ignore
    __tablename__ = "wbrugere"


class Bruger(Base, BaseBruger):  # type: ignore
    __tablename__ = "brugere"


class BaseEnhed(Compare):
    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    navn = Column(String(250), nullable=False)
    bvn = Column(String(250), nullable=False)
    forældreenhed_uuid = Column(String(36), primary_key=False)
    enhedstype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    enhedstype_titel = Column(String(250), nullable=False)
    enhedsniveau_uuid = Column(String(36))  # ForeignKey('klasser.uuid'))
    enhedsniveau_titel = Column(String(250))
    tidsregistrering_uuid = Column(String(36))  # ForeignKey('klasser.uuid'))
    tidsregistrering_titel = Column(String(250))
    organisatorisk_sti = Column(String(1000))
    leder_uuid = Column(String(36))
    fungerende_leder_uuid = Column(String(36))
    opmærkning_uuid = Column(String(36))  # Export UUID of "org_unit_hierarchy"
    opmærkning_titel = Column(String(250))  # Export title of "org_unit_hierarchy"
    startdato = Column(String(10))
    slutdato = Column(String(10))


class WEnhed(Base, BaseEnhed):  # type: ignore
    __tablename__ = "wenheder"


class Enhed(Base, BaseEnhed):  # type: ignore
    __tablename__ = "enheder"


class BaseAdresse(Compare):
    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bvn = Column(String(250))
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


class WAdresse(Base, BaseAdresse):  # type: ignore
    __tablename__ = "wadresser"


class Adresse(Base, BaseAdresse):  # type: ignore
    __tablename__ = "adresser"


class BaseEngagement(Compare):
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


class WEngagement(Base, BaseEngagement):  # type: ignore
    __tablename__ = "wengagementer"


class Engagement(Base, BaseEngagement):  # type: ignore
    __tablename__ = "engagementer"


class BaseTilknytning(Compare):
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
    faglig_organisation = Column(String(250), nullable=True)


class WTilknytning(Base, BaseTilknytning):  # type: ignore
    __tablename__ = "wtilknytninger"


class Tilknytning(Base, BaseTilknytning):  # type: ignore
    __tablename__ = "tilknytninger"


class BaseOrlov(Compare):
    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    bvn = Column(String(250), nullable=False)
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    orlovstype_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    orlovstype_titel = Column(String(250), nullable=False)
    engagement_uuid = Column(String(36))
    startdato = Column(String(10))
    slutdato = Column(String(10))


class WOrlov(Base, BaseOrlov):  # type: ignore
    __tablename__ = "worlover"


class Orlov(Base, BaseOrlov):  # type: ignore
    __tablename__ = "orlover"


class BaseItSystem(Compare):
    uuid = Column(String(36), nullable=False, primary_key=True)
    navn = Column(String(250), nullable=False)


class WItSystem(Base, BaseItSystem):  # type: ignore
    __tablename__ = "wit_systemer"


class ItSystem(Base, BaseItSystem):  # type: ignore
    __tablename__ = "it_systemer"


class BaseItForbindelse(Compare):
    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    it_system_uuid = Column(String(36))  # , ForeignKey('it_systemer.uuid'))
    bruger_uuid = Column(String(36))  # , ForeignKey('brugere.uuid'))
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    brugernavn = Column(String(250))
    primær_boolean = Column(Boolean)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class WItForbindelse(Base, BaseItForbindelse):  # type: ignore
    __tablename__ = "wit_forbindelser"


class ItForbindelse(Base, BaseItForbindelse):  # type: ignore
    __tablename__ = "it_forbindelser"


class BaseLeder(Compare):
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


class WLeder(Base, BaseLeder):  # type: ignore
    __tablename__ = "wledere"


class Leder(Base, BaseLeder):  # type: ignore
    __tablename__ = "ledere"


class BaseLederAnsvar(Compare):
    id = Column(Integer, nullable=False, primary_key=True)
    leder_uuid = Column(String(36))  # , ForeignKey('ledere.uuid'))
    lederansvar_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    lederansvar_titel = Column(String(250), nullable=False)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class WLederAnsvar(Base, BaseLederAnsvar):  # type: ignore
    __tablename__ = "wleder_ansvar"


class LederAnsvar(Base, BaseLederAnsvar):  # type: ignore
    __tablename__ = "leder_ansvar"


class BaseKLE(Compare):
    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    enhed_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    kle_aspekt_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    kle_aspekt_titel = Column(String(250), nullable=False)
    kle_nummer_uuid = Column(String(36))  # , ForeignKey('klasser.uuid'))
    kle_nummer_titel = Column(String(250), nullable=False)
    startdato = Column(String(10))
    slutdato = Column(String(10))


class WKLE(Base, BaseKLE):  # type: ignore
    __tablename__ = "wkle"


class KLE(Base, BaseKLE):  # type: ignore
    __tablename__ = "kle"


class Kvittering(Base):  # type: ignore
    __tablename__ = "kvittering"

    id = Column(Integer, nullable=False, primary_key=True)
    query_tid = Column(DateTime)
    start_levering_tid = Column(DateTime)
    slut_levering_tid = Column(DateTime)


class BaseEnhedssammenkobling(Compare):
    id = Column(Integer, nullable=False, primary_key=True)
    uuid = Column(String(36), nullable=False)
    enhed1_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    enhed2_uuid = Column(String(36))  # , ForeignKey('enheder.uuid'))
    startdato = Column(String(10))
    slutdato = Column(String(10))


class WEnhedssammenkobling(Base, BaseEnhedssammenkobling):  # type: ignore
    __tablename__ = "wenhedssammenkobling"


class Enhedssammenkobling(Base, BaseEnhedssammenkobling):  # type: ignore
    __tablename__ = "enhedssammenkobling"


class BaseDARAdresse(Compare):
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


class WDARAdresse(Base, BaseDARAdresse):  # type: ignore
    __tablename__ = "wdar_adresser"


class DARAdresse(Base, BaseDARAdresse):  # type: ignore
    __tablename__ = "dar_adresser"


sql_type = (
    Facet
    | Klasse
    | Bruger
    | Enhed
    | Adresse
    | Engagement
    | Tilknytning
    | Orlov
    | ItSystem
    | ItForbindelse
    | Leder
    | LederAnsvar
    | KLE
    | Enhedssammenkobling
    | DARAdresse
)
