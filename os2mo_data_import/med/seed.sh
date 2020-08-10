#!/bin/bash

CLI="python mox_util.py cli"

$CLI ensure-facet-exists --bvn hoved_organisation --description Hovedorganisation
$CLI ensure-facet-exists --bvn faglig_organisation --description "Faglig organisation"

$CLI ensure-class-exists --bvn DA --title "DA" --description "Dansk Arbejdsgiverforening" --facet-bvn hoved_organisation

declare -A DA

DA[DA_asfalt]="Asfaltindustrien/Drivkraft Danmark"
DA[DA_byggeri]="Dansk Byggeri"
DA[DA_erhverv]="Dansk Erhverv Arbejdsgiver"
DA[DA_textil]="Dansk Textil & Beklædning"
DA[DA_maler]="Danske Malermestre"
DA[DA_medier]="Danske Mediers Arbejdsgiverforening"
DA[DA_DI]="DI - Organisation for erhvervslivet"
DA[DA_green]="Foreningen af Danske Virksomheder i Grønland"
DA[DA_grakom]="Grakom Arbejdsgivere"
DA[DA_horesta]="Horesta Arbejdsgiver"
DA[DA_rederi]="DanskeRederier"
DA[DA_sama]="SAMA - Sammenslutningen af mindre Arbejdsgiverforeninger i Danmark"
DA[DA_tekniq]="TEKNIQ Installatørernes Organisation"

for BVN in "${!DA[@]}"; do
    TITLE="${DA[$BVN]}"
    $CLI ensure-class-exists --bvn ${BVN} --title "${TITLE}" --facet-bvn faglig_organisation --parent-bvn DA
done

$CLI ensure-class-exists --bvn LO --title "LO" --description "Landsorganisationen i Danmark" --facet-bvn hoved_organisation

declare -A LO
LO[LO_blik]="Blik og Rørarbejderforbundet i Danmark"
LO[LO_daf]="Dansk Artist Forbund (DAF)"
LO[LO_el]="Dansk EL-Forbund"
LO[LO_kosmetik]="Dansk Frisør og Kosmetiker Forbund"
LO[LO_service]="Dansk Funktionærforbund - Serviceforbundet"
LO[LO_djf]="Dansk Jernbaneforbund (DJF)"
LO[LO_metal]="Dansk Metal"
LO[LO_3f]="Fagligt Fælles Forbund (3F)"
LO[LO_foa]="FOA - Fag og Arbejde"
LO[LO_jail]="Fængselsforbundet i Danmark"
LO[LO_hk]="HK/Danmark"
LO[LO_korporal]="Hærens Konstabel- og Korporalforening"
LO[LO_spiller]="Håndbold Spiller Foreningen"
LO[LO_maler]="Malerforbundet i Danmark"
LO[LO_nnf]="Fødevareforbundet NNF"
LO[LO_sl]="Socialpædagogernes Landsforbund (SL)"
LO[LO_spf]="Spillerforeningen (SPF)"
LO[LO_tl]="Teknisk Landsforbund (TL)"

for BVN in "${!LO[@]}"; do
  TITLE="${LO[$BVN]}"
  $CLI ensure-class-exists --bvn ${BVN} --title "${TITLE}" --facet-bvn faglig_organisation --parent-bvn LO
done

$CLI ensure-facet-exists --bvn LO_3f_hovedomroede --description "3F Hovedområde"

$CLI ensure-class-exists --bvn LO_3f_industri --title "Industrigruppen" --facet-bvn LO_3f_hovedomroede --parent-bvn LO_3f
$CLI ensure-class-exists --bvn LO_3f_transport --title "Transportgruppen" --facet-bvn LO_3f_hovedomroede --parent-bvn LO_3f
$CLI ensure-class-exists --bvn LO_3f_offentlig --title "Den Offentlige Gruppe" --facet-bvn LO_3f_hovedomroede --parent-bvn LO_3f
$CLI ensure-class-exists --bvn LO_3f_bygge --title "Byggegruppen" --facet-bvn LO_3f_hovedomroede --parent-bvn LO_3f
$CLI ensure-class-exists --bvn LO_3f_green --title "Den Grønne Gruppe" --facet-bvn LO_3f_hovedomroede --parent-bvn LO_3f
$CLI ensure-class-exists --bvn LO_3f_privat --title "Privat, Service, Hotel & Restauration" --facet-bvn LO_3f_hovedomroede --parent-bvn LO_3f

# Configure MO to utilize newly created facet
TOP_LEVEL_UUID=$($CLI ensure-facet-exists --bvn hoved_organisation --description Hovedorganisation | cut -f1 -d' ')
curl -X POST -H "Content-Type: application/json" --data "{\"org_units\": {\"association_dynamic_facets\": \"${TOP_LEVEL_UUID}\"}}" http://localhost:5000/service/configuration
