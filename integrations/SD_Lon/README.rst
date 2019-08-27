Integration til SD Løn
======================

Indledning
----------
Denne integration gør det muligt at hente og opdatere organisations- og
medarbejderoplysninger fra SD Løn til OS2MO. 

Opsætning
---------

For at kunne afvikle integrationen, kræves loginoplysninger til SD-Løn, som angives
via miljøvariable i den terminal integrationen afvikles fra. Disse miljøvariable er:

 * ``INSTITUTION_IDENTIFIER``: Institution Identifer i SD.
 * ``SD_USER``: Brugernavn (inklusiv foranstille SY) til SD.
 * ``SD_PASSWORD``: Password til SD.


Detaljer om importen
--------------------
Udtræk fra SD Løn foregår som udgangspunkt via disse webservices:

 * ``GetOrganization20111201``
 * ``GetDepartment20111201``
 * ``GetPerson20111201``
 * ``GetEmployment20111201``
  
Det er desuden muligt at køre et udtræk som synkroniserer ændringer som er meldt ind
til SD Løn, men endnu ikke har nået sin virkningsdato:

 * ``GetEmploymentChanged20111201``
 * ``GetPersonChangedAtDate20111201``

Endelig er der også en implementering af løbende synkronisering af ændringer i SD
Løn, til dette anvendes udover de nævne webservices også:

 * ``GetEmploymentChangedAtDate20111201``
  
Alle enheder fra SD importeres 1:1 som de er i SD, dog er det muligt at flytte enheder
uden hverken overenhed eller underenheder til en særlig overenhed kaldet
'Forældreløse enheder'.

Medarbejdere som er ansat på niveauerne, Afdelings-niveau og NY1-niveau rykkes op til
det laveste niveau højere end dette, og der oprettes en tilknytning til den afdeling
de befinder sig i i SD Løn.

Det er muligt at levere en ekstern liste med ledere, eller alternativt at benytte SD
Løns JobPositionIdentifier til at vurdere at en medarbejder skal regnes som leder.

Medarbejdere i statuskode 3 regnes for at være på orlov.

En medarbejders primære ansættelse regnes som den ansættelse som har den største
arbejdstidsprocent, hvis flere har den samme, vælges ansættelsen med det laveste
ansættelsenummer. Hvis ingen ansættelse har en arbejdstidsprocent større end nul,
regnes ingen engagementer som primær.

Der importeres ingen adresser på medarbejdere. På sigt vil disse kunne hentes fra
ad-integrationen.

Alle personer og ansættelser som kan returneres fra de ovennævnte webservices
importeres, både passive og aktive. Dette skyldes dels et ønske om et så komplet
datasæt som muligt, dels at SDs vedligeholdsesservices gå ud fra, at alle kendte
engagementer er i den lokale model.

Postadresser på enheder hentes fa SD og valideres mod DAR. Hvis adressen kan entydigt
genkendes hos DAR, gemmes den tilhørende DAR-uuid på enheden i MO.

Email adresser og p-numre importeres fra SD hvis disse findes for enheden.

Vi importerer UUID'er på enheder fra SD til MO så enheder i MO og SD har samme UUID.

Medarbejdere har ikke en UUID i SD, så her benyttes cpr som nøgle på personen og
ansættelsesnummeret som nøgle på engagementer


Tjekliste for fuldt import
--------------------------

1. Kør importværktøjet med fuld historik (dette er stanard opførsel).
2. Kør sd_fix_organisation.py for at sikre synkronisering af alle enheder
3. Kør en inledende ChangedAt for at hente alle kendte fremtidige ændringer og intitialisere den lokale database over kørseler.
4. Kør sd_changed_at.py periodisk (eksempelvis dagligt). Hvis enhederne har ændret sig, er det nødvendigt først at køre sd_fix_organisation.py før hver kørsel.
