************************
Integration til OPUS Løn
************************


Indledning
==========
Denne integration gør det muligt at hente og opdatere organisations- og
medarbejderoplysninger fra XML dumps fra OPUS Løn til OS2MO

Opsætning
=========

For at kunne afvikle integrationen, kræves adgang til en mappe med xml dums fra OPUS.
Oplysninger om stien til denne mapper er øjeblikket skrevet direkte i importkoden og
kan ikke ændres i runtime.

Den forventede sti for mappen med opus dumps er:
``/opt/magenta/dataimport/opus``

De enkelte dumps forventes at være navngivet systematisk som:
``ZLPE<data + tid>_delta.xml``

Eksempelvis ``ZLPE20190902224224_delta.xml``.


Nuværende implementeringslogik for import fra Opus:
===================================================

 * Data indlæses i form at et xml-dump.
 * Hvis data indeholder information om enhedstyper, oprettes disse enhedstyper som
   klasser, hvis ikke, får alle enheder typen ``Enhed``.
 * SE-, CVR-, EAN- og p-numre og telefon indlæses på enheder, hvis disse oplysninger
   tilgængelige.
 * Hvis data indeholder postadresser på enheder eller medarejdere, slås disse
   adresser op på DAR, og hvis det er muligt at få en entydigt match, gemmes
   DAR-uuid'en på enheden eller personen.
 * Telefon og email importeres for medarbejdere, hvis de findes i data.
 * Ansættelsestyper og titler oprettes som klasser og sættes på de tilhørende
   engagementer.
 * Information om ledere importeres direkte fra data, de to informationer
   ``superiorLevel`` og ``subordinateLevel`` konkateneres til et lederniveau.
 * Information om roller importeres direkte fra data.

IT-Systemer
===========

En import fra OPUS vil oprette IT-systemet 'Opus' i MO. Alle medarbejdere som har
en værdi i feltet ``userId`` vil få skrevet deres OPUS brugernavn på dette
IT-system.

.. _AD Integration til SD Opus:

AD-Integration
==============

OPUS Importen understøtter at anvende komponenten `Integration til Active Directory`_
til at berige objekterne fra OPUS med information fra Active Directory. I øjebliket
er det muligt at importere felterne ``ObjectGuid`` og ``SamAccountName``.

Hvis AD integrationen er aktiv, vil importeren oprette IT-systemet 'Active Directory'
og oprette alle brugere der findes i AD med bruernavnet fundet i ``SamAccountName``.
Brugere med en AD konto vil blive oprettet med deres AD ``ObjectGuid`` som UUID på
deres brugerobjekt.
