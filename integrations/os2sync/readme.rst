********************************
Integration til STS Organisation
********************************

Indledning
==========
Denne integration gør det muligt at opdatere STS Organisation fra OS2MO

Opsætning
=========

For at kunne afvikle integrationen kræves en række opsætninger af den lokale server.

Opsætningen i ``settings.json``

fælles parametre
----------------

 * ``mora.base``: Beskriver OS2MO's adresse
 * ``crontab.SAML_TOKEN``: api token for service-adgang til OS2MO
 * ``municipality.cvr`` : Kommunens CVR-nummer
 * ``crontab.RUN_OS2SYNC``: Bestemmer om jobbet skal køres i cron (true/false) 


os2syncs parametre
------------------

 * ``os2sync.log_file``: logfil, typisk 
   '/home/bruger/CRON/os2sync.log'
 * ``os2sync.log_level``: Loglevel, numerisk efter pythons logging-modul,
    typisk 10, som betyder at alt kommer ud
 * ``os2sync.ca_verify_os2mo``: Angiver om OS2mo serverens certifikat skal checkes,
    typisk true
 * ``os2sync.ca_verify_os2sync``: Angiver om Os2Sync containerens certifikat skal checkes,
    typisk true
 * ``os2sync.hash_cache``: Cache som sørger for at kun ændringer overføres
 * ``os2sync.phone_scope_classes``: Begrænsning af hvilke telefon-klasser, der kan komme op,
    anvendes typisk til at frasortere hemmelige telefonnumre
 * ``os2sync.email_scope_classes``: Begrænsning af hvilke email-klasser, der kan komme op,
    anvendes typisk til at frasortere hemmelige email-addresser
 * ``os2sync.api_url``: Adresse på os2sync-containeres API, typisk
    http://localhost:8081/api
 * ``os2sync.top_unit_uuid``: Den top level organisation, der skal overføres,
    typisk Kommunenavn Kommune
 * ``os2sync.xfer_cpr``: Bestemmer om cpr skasl overføres, typisk true
 * ``os2sync.use_lc_db``: Bestemmer om kørslen skal anvende lora-cache for hastighed
 * ``os2sync.ignored.unit_levels``: liste af unit-level-klasser,
    der skal ignoreres i overførslen
 * ``os2sync.ignored.unit_types``: liste af unit-type-klasser,
    der skal ignoreres i overførslen

