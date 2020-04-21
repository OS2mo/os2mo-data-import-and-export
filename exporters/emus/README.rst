****************
Eksport til EMUS
****************

Indledning
==========

Dette program eksporterer xml som kan indlæses i emus - også kaldet musskema.dk
og sender dem efter endt programafvikling 2 steder hen:

 * Til 'exports'-directoriet. som kan nås igennem OS2MO's frontend
 * Via sftp til musskema.dk. 

Programkomponenter
==================

Programmet er udført i to afdelinger, opstået i forbindelse med implenteringen,
hvor vi staertede med at udveksle xml-filer uden automatiseret sftp.

* viborg_xml_emus.py er den komponent, der skaber xml-outputtet
* viborg_xml_emus_sftp.py er den komponent, der sørger for at sende 
  outputtet til de to destinationer

Indhold af udtrækket
====================

Udtrækket indeholder 

* Medarbejderes engagementer
* Organisatoriske enheder
* Ledere


Medarbejderes engagementer
--------------------------

Denne del af udtrækket indeholder for hver ansættelse start, slut, cpr, navn, 
adresse, telefon, engagementstype, tjenestenummer, email og telefon
og brugernavn i valgte IT-system' Herudover er der et ``client``-felt, som er ``1`` 
for medabejdere i denne del af udtrækket.

Telefon og email udlades hvis scope er 'SECRET'

Nøglen, ``employee_id``, som overføres er tjenestenr.

Timelønnede medarbejdere er ikke med i udtrækket.


Organisatoriske enheder
-----------------------

Denne del af udtrækket indeholder for hver afdeling navn, adresse, telefonnummer,
leder og afdelingens tidsgyldighed.

Organisatoriske enheder, som kun har timelønnede medarbejdere er ikke med i udtrækket.

Ledere
------

Ledere ligner medarbejderne, men har anderledes værdier i client og employee_id.
Feltet ``client`` er for ledere hårdkodet til 540.  
Nøglen, ``employee_id``, som overføres er lederens eget , ikke engagementets uuid.

Telefon og email udlades hvis scope er 'SECRET'

Lederengagementer uden en tilknyttet person er ikke med i udtrækket.

Ledereengagementer, som ikke er af ``EMUS_RESPONSIBILITY_CLASS`` kommer ikke med i udtrækket.


Styring af udtrækket
====================

Programmet er afhængig af følgende indstillinger i ``settings.json``

 * ``crontab.MORA_BASE`` styrer hvilken OS2MO, der tilgås 
 * ``crontab.MORA_ROOT_ORG_UNIT_NAME`` angiver roden af det organisatoriske træ, der skal overføres
 * ``crontab.USERID_ITSYSTEM`` angiver hvilket IT-system, man tager brugernavnet fra, default er ``Active Directory``.
 * ``crontab.EMUS_RESPONSIBILITY_CLASS`` angiver den leder-klasse man vil overføre
 * ``crontab.EMUS_FILENAME`` default ``emus_filename.xml`` er det filnavn viborg_xml_emus.py kan skrive til
 * ``crontab.SFTP_USER`` angiver den sftp-user, man forbinder som
 * ``crontab.SFTP_HOST`` er typisk ``sftp.serviceplatformen.dk``, men til test kan en anden anvendes
 * ``crontab.SFTP_KEY_PATH`` angiver den nøgle, man anvender got at forbinde sig til sftp-serveren
 * ``crontab.SFTP_KEY_PASSPHRASE`` angiver password til ovenst. nøgle
 * ``crontab.MUSSKEMA_RECIPIENT`` angiver den bruger, som man skal sende til. Dette giver kun mening med sftp på serviceplatformen
 * ``crontab.QUERY_EXPORT_DIR`` angiver det sted, hvor kopien af rapporten skal lægges - dette skal være det output-dir, som kan nås igennem OS2MO.


Programmet kan styres af følgende environment-variable:

 * ``LOG_LEVEL`` kan anvendes til at få mere log ud under afviklingen, hvis man sætter den til strengen DEBUG
