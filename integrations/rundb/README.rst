rundb
=====

For at holde rede på hvornår MO sidst har hentet ekstern data, findes en SQLite
database som indeholder to rækker for hver færdiggjort kørsel. Adressen på denne
database er angivet i settings med nøglen:
* ``integrations.SD_Lon.import.run_db`` for SD
* ``integrations.opus.import.run_db`` for OPUS

Programmet ``db_overview.py`` er i stand til at læse denne database og giver et
outut som dette:

::

   id: 1, from: 2019-08-22 00:00:00, to: 2019-08-22 00:00:00, status: Running since 2019-08-22 14:03:01.226492
   id: 2, from: 2019-08-22 00:00:00, to: 2019-08-22 00:00:00, status: Initial import: 2019-08-22 16:31:29.151569
   id: 3, from: 2019-08-22 00:00:00, to: 2019-08-23 00:00:00, status: Running since 2019-08-23 09:00:04.215068
   id: 4, from: 2019-08-22 00:00:00, to: 2019-08-23 00:00:00, status: Update finished: 2019-08-23 09:05:36.587527
   id: 5, from: 2019-08-23 00:00:00, to: 2019-08-24 00:00:00, status: Running since 2019-08-28 08:44:11.181134
   id: 6, from: 2019-08-23 00:00:00, to: 2019-08-24 00:00:00, status: Update finished: 2019-08-28 08:46:19.146615
   id: 7, from: 2019-08-24 00:00:00, to: 2019-08-25 00:00:00, status: Running since 2019-08-28 08:49:27.479475
   id: 8, from: 2019-08-24 00:00:00, to: 2019-08-25 00:00:00, status: Update finished: 2019-08-28 08:49:36.189767
   id: 9, from: 2019-08-25 00:00:00, to: 2019-08-26 00:00:00, status: Running since 2019-08-28 08:50:42.929468
   id: 10, from: 2019-08-25 00:00:00, to: 2019-08-26 00:00:00, status: Update finished: 2019-08-28 08:50:51.811845
   id: 11, from: 2019-08-26 00:00:00, to: 2019-08-27 00:00:00, status: Running since 2019-08-28 08:54:46.207228
   id: 12, from: 2019-08-26 00:00:00, to: 2019-08-27 00:00:00, status: Update finished: 2019-08-28 08:59:20.876762
   id: 13, from: 2019-08-27 00:00:00, to: 2019-08-28 00:00:00, status: Running since 2019-08-28 09:07:25.961710
   id: 14, from: 2019-08-27 00:00:00, to: 2019-08-28 00:00:00, status: Update finished: 2019-08-28 09:12:08.191701

Ved starten af import kørsler, skrives en linje med status ``Running`` og
efter hver kørsel skrives en linje med status ``Update finished``.

Der kan dermed blokeres før følgende kørsler hvis forrige linje ikke er
afsluttet endnu, da dette betyder at integrationen stadig kører eller at forrige
kørsel fejlede.
