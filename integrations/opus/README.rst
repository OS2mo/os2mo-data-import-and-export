Integration til OPUS Løn
======================

Indledning
----------
Denne integration gør det muligt at hente og opdatere organisations- og
medarbejderoplysninger fra XML dumps fra OPUS Løn til OS2MO

Opsætning
---------

For at kunne afvikle integrationen, kræves loginoplysninger adgang til en
mappe med xml dums fra OPUS. Disse oplysinger er dog i øjeblikket skrevet
direkte i importkoden kan ikke ændres i runtime.



Nuværende implementeringslogik for import fra Opus:
---------------------------------------------------

 * Data indlæses i form at et xml-dump.
 * Hvis data indeholder information om enhedstyper, oprettes disse enhedstyper som klasser, hvis ikke, får alle enheder typen ``Enhed``.
 * SE-, CVR-, EAN- og p-numre og telefon indlæses på enheder, hvis disse oplysninger tilgængelige.
 * Hvis data indeholder postadresser på enheder eller medarejdere, slås disse adresser op på DAR, og hvis det er muligt at få en entydigt match, gemmes DAR-uuid'en på enheden eller personen.
 * Telefon og email importeres for medarbejdere, hvis de findes i data.
 * Ansættelsestyper og titler oprettes som klasser og sættes på de tilhørende engagementer.
 * Information om ledere importeres direkte fra data, de to informationer ``superiorLevel`` og ``subordinateLevel`` konkateneres til et lederniveau.
 * Information om roller importeres direkte fra data.
