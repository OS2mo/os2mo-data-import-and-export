**********************
Eksport til plan2learn
**********************

Indledning
==========
Denne eksport script bygger datafiler som via sftp kan sendes til plan2learn.


Implementeringsstrategi
=======================

Der udarbejdes i alt 5 csv udtræk:

 * `bruger.csv`: Udtræk af alle nuværende og kendte fremtidigt aktive brugere i
   kommunen.
 * `organisation.csv`:  Udtræk af alle organisationsenheder og deres relation til
   hinanden.
 * `engagement.csv`: Udtræk over alle nuværende og kendte fremtidige engagementer.
 * `stillingskode.csv`: Udtræk over alle aktive stillingsbetegnelser.
 * `leder.csv`: Udtræk over alle ledere i kommunen.



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


Engagement
==========

 * `BrugerId`: Brugerens uuid i MO. Nøgle til `Bruger` -udtrækket.
 * `AfdelingsId`: Afdelingens uuid i MO. Nøgle til `Organisation` -udtrækket.
 * `AktivStatus`: Sættes til 1 for aktive engagementer, 0 for fremtidige.
 * `StillingskodeId`: uuid til engagements titel, som gemmes som en klasse under
   facetten `engagement_job_function` i MO. Nøgle til stillingskode.
 * `Primær`: 1 hvis engagementet er primært, ellers 0.
 * `Engagementstype`: Angiver om stillingen er måneds eller timelønnet.
 * `StartdatoEngagement`: Startdato hvis engagementet endnu ikke er startet


Kun timelønnede og og månedslønnede engagementer eksporteres. Angivelse af hvordan
disse engagementstyper gives er i øjeblikket givet ved en liste direkte i koden,
dette skal flyttes til settings. TODO!

Engagmenter som tidligere har været aktive, men som nu er afsluttede, eksporteres
ikke. Kendte fremtidige engagementer eksporteres med AktivStatus 0.


Stillingskode
=============

 * `StillingskodeID`: uuid på den klasse i MO som holder stillingsbetegnelsne,
   nøgle til `Engagement` -udtrækket
 * `AktivStatus`: Angiver om stillingskoden anvendes. Der eksporteres kun akive
   stillingskoder, så værdien er altid 1.
 * `Stillingskode`: Læsbar tekstrepræsentation af stillingskoden (i modsæting til
   uuid'en).
 * `Stillingskode#`: I øjeblikket en Kopi af `StillingskodeID`.


Leder
=====

 * `BrugerId`: Brugerens uuid i MO. Nøgle til `Bruger` -udtrækket.
 * `AfdelingsID`: Afdelingens uuid i MO. Nøgle til `Organisation` -udtrækket.
 * `AktivStatus`: Kun aktive ledere eksporteres, væriden er altid 1.
 * `Titel`: Lederens ansvarsområder.
