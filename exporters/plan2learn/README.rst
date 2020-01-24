**********************
Eksport til plan2learn
**********************

Indledning
==========
Denne eksport script bygger datafiler som via sftp kan sendes til plan2learn.


Implementeringsstrategi
=======================

Der udarbejdes i alt 5 csv udtræk:

 * `plan2learn_bruger.csv`: Udtræk af alle nuværende og kendte fremtidigt aktive brugere i kommunen.



Brugerudtrækket
===============

I dette udtræk eksporteres disse feler:
 * `BrugerId`: Brugerens uuid i MO
 * `CPR`: Brugerens cpr-nummer
 * `Navn`: Brugerens fulde navn, ikke opdelt i fornavn og efteranvn
 * `E-mail`: Hvis brugeren har en email i MO, angives den her.
 * `Mobil`: Hvis brugeren har en mobiltelefon i MO, angives den her.

Mobiltelefon genkenes via en besemt klasse under adressetype, denne klasse er
for nuværende hårdkodet direkte i python filen, men vil på sigt blive flyttet til
`settings.json`.
   
Kun personer med ansættelsestype Timeløn eller Månedsløn inkluderes i udtrækket.
Disse typer genkendes via en liste med de to uuid'er på typerne, for nuværende er
listen hårdkoden direkte i python filen, men vil på sigt blive flyttet til
`settings.json`.


Organisation
============

 * `AfdelingsID`: Afdelingens uuid i MO.
 * `Afdelingsnavn`: Afdelingens navn.
 * `Parentid`: uuid på enhedens forældreenhed.
 * `Gade`: Gadenavn
 * `Postnr`: Postnummer
 * `By`: Bynavn

Kun enheder på strukturniveau eksporteres. Dette foregår på den måde, at hvis enheden
har et enhedsniveau (`org_unit_level`) som figurerer i nøglen
`integrations.SD_Lon.import.too_deep` i `settings.json` vil enheden blive ignoreret.

Enheder som ikke har en gyldig adresse i MO, vil få angivet en tom streng for Gade,
Postnr og By.

Rodenheden for organisationen vil have en tom streng som Parentid.
