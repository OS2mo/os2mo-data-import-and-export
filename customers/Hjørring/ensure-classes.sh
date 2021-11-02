#!/bin/bash
. tools/job-runner.sh
. venv/bin/activate

#AD addresses:
metacli ensure_class_exists --bvn="AD Email" --scope=EMAIL --facet=employee_address_type
metacli ensure_class_exists --bvn="AD Mobiltelefon" --scope=PHONE --facet=employee_address_type
metacli ensure_class_exists --bvn="AD Telefon" --scope=PHONE --facet=employee_address_type

#org unit types:
org_unit_types=(
    "Hovedudvalg" 
    "Område-MED" 
    "Sektor MED" 
    "Distrikt MED" 
    "Arbejdsplads MED" 
    "Personalemøde mm status" 
    "Arbejdsmiljøgruppe"
    )
for i in "${org_unit_types[@]}"; do
    echo "$i"
    metacli ensure_class_exists --bvn="$i" --facet=org_unit_type
done

#role_types:
role_types=(
    "Arbejdsmiljørepræsentant"
    "Medarbejderrepræsentant"
    "Sekretariat"
    "Ledelsesrepræsentant"
    "Næstformand"
    "Formand"
    "Medlem"
    "Tillidsrepræsentant"
    "Leder AM-gruppe"
)
for i in "${role_types[@]}"; do
    echo "$i"
    metacli ensure_class_exists --bvn="$i" --facet=association_type
done
