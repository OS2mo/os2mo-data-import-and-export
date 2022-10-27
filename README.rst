#################
OS2MO Data Import
#################

Magentas officielle repo til integrationer og eksportfunktioner til OS2MO.

For spørgsmål til koden eller brug af den, er man velkommen til at kontakte
Magenta ApS <info@magenta.dk>

Usage
-----
Start en OS2mo stak vha. `docker-compose`, se detaljer her:
* https://os2mo.readthedocs.io/en/1.16.1/dev/environment/docker.html?#docker-compose

Dipex' dockerimage kan bygges med `docker-compose build`.
Når dette er sket, kan DIPEX kommandoer kaldes med fx:
```
docker-compose run --rm dipex python3 metacli.py 
```
Alternativt kan man starte et udviklingsmiljø med:
```
docker-compose up -d --build
```
Når kommandoen er kørt færdig, kan man hoppe ind i containeren med:
```
docker-compose exec dipex /bin/bash
```
Dette giver en terminal i containeren, hvorfra diverse programmer kan køres.
Et fælles entrypoint til programmerne findes ved at køre:
```
python3 metacli.py
```
Forbindelsen imod OS2mo, kan testes med programmet: `check_connectivity`:
```
python3 metacli.py check_connectivity --mora-base http://mo
```

Dependencies
------------
Der bruges poetry til at håndtere pakker. For at sikre at all bruger samme version kan man gøre det gennem docker, fx:

```
docker-compose run --rm dipex poetry update
```
For at dette kan virke er filerne pyproject.toml og poetry.lock mountet med skriveadgang i docker-compose.yml.