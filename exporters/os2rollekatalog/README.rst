***************************
Eksport til OS2Rollekatalog
***************************

Indledning
==========

Dette program udfylder OS2Rollekataloget med organisation og medarbejdere fra OS2MO. 
Kræver opsætning af AD i settings, og der skrives kun brugere til OS2Rollekataloget 
der findes både i OS2MO og i AD.

Eksporten sender alle informationerne i ét payload som overskriver det der ligger i rollekataloget i forvejen.


Konfiguration
=============

For at anvende eksporten er det nødvendigt at oprette et antal nøgler i
`settings.json`:

 * ``exporters.os2rollekatalog.rollekatalog.url``: URL adressen til rollekatalogets organisations api, 
   fx. https://os2mo.rollekatalog.dk/api/organisation/v3
 * ``exporters.os2rollekatalog.rollekatalog.api_token``: API token til autentificering med rollekataloget
 * ``exporters.os2rollekatalog.main_root_org_unit``: UUID på rod-enheden i rollekataloget.
 * ``exporters.os2rollekatalog.ou_limit_uuid``: Optionelt: UUID på rod-enheden i MO. 
    Begrænser eksporten til kun at sende enheder der hører under den givne organisationsenhed
    samt medarbejdere der har et engagement i en af de enheder.
 

Eksporteret data
================

Payloaded til OS2Rollekatalog er opdelt i Enheder og Medarbejdere og indeholder følgende data:


Medarbejdere
------------

* UUID
* Navn
* Email
* AD brugernavn
* Engagementer (Stillingsbetegnelse og Enheds_uuid)

Der gives en advarsel i loggen ved mere end én email adresse på en bruger.

Organisatoriske enheder
-----------------------

* UUID
* Navn
* Email
* Overenhed
* Leder 

Der tillades kun én leder pr. Organisatorisk enhed.
