#!/bin/bash

MOX_IP=$(az container show --resource-group os2mo-rg --name terraform-mox --query ipAddress.ip --output tsv)
MO_IP=$(az container show --resource-group os2mo-rg --name terraform-mo --query ipAddress.ip --output tsv)
IPS="\"mox.base\": \"http://$MOX_IP/\",
    \"mora.base\": \"http://$MO_IP/\"}"

az container create --resource-group os2mo-rg --name dipex \
--image reg.magenta.dk/rammearkitektur/os2mo-data-import-and-export/feature:feature_dipex_in_docker \
--registry-password $REG_PASS \
--registry-username blj@magenta-aps.dk \
--registry-login-server reg.magenta.dk \
--location westeurope \
--vnet terraform-virtual-network \
--subnet terraform-subnet-mo \
--secrets settings.json="$(cat settings/silkeborg-settings.json) ,$IPS"  cpr_uuid_map.csv=" " \
--secrets-mount-path /code/settings


echo $IPS
