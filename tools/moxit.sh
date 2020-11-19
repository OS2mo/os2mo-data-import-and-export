#!/bin/bash
# Du vil tilføje et itsystem
# Check selv at klassen med den bvn ikke findes i forvejen
# Kald nu dette script med:
#
# scope=TEXT bvn=noegle title="mytitle" facet=time_planning bash moxklas.sh
#
# tilføj en parameter for at få et dry_run check
#
# scriptet finder selv uuider for facet og organisation

UUID="b0c27020-9a7a-11ea-8b83-0bb60cd4e329"

itsystem_json(){
	bvn=$1
	titel=$2
	organisation=$3
	(cat << ITSYSTEM
{
  "tilstande": {
    "itsystemgyldighed": [
      {
        "gyldighed": "Aktiv",
        "virkning": {
          "to": "infinity",
          "from": "1930-01-01"
        }
      }
    ]
  },
  "relationer": {
    "tilhoerer": [
      {
        "virkning": {
          "to": "infinity",
          "from": "1930-01-01"
        },
        "uuid": "${organisation}"
      }
    ]
  },
  "attributter": {
    "itsystemegenskaber": [
      {
        "itsystemnavn": "${titel}",
        "virkning": {
          "to": "infinity",
          "from": "1930-01-01"
        },
        "brugervendtnoegle": "${bvn}"
      }
    ]
  }
}
ITSYSTEM
)}

opret(){
    uuid=${1}
    curl --header "Content-Type: application/json" -X PUT http://localhost:8080/organisation/itsystem/${uuid} -d @-
}

#bvn="$1"
#titel="$2"
dry_run="$1"
organisation=$(curl http://localhost:8080/organisation/organisation?bvn=% | jq -r .results[][])


existing=$(curl http://localhost:8080/organisation/itsystem?bvn=${bvn} | jq -r .results[][])
if [ -n "${existing}" ]; then
    echo "Brugervendt nøgle eksisterer"
    exit 1
fi

[ -n "${bvn}" -a -n "${titel}" -a -n "${uuid}" -a -z "${dry_run}" ] && (
    itsystem_json "${bvn}" "${titel}" "${organisation}" | opret ${uuid}
) || (

    echo DRY_RUN CHECK: Du har angivet:

    echo uuid: $uuid
    echo bvn: $bvn
    echo titel: $titel

    echo DRY_RUN CHECK - postdata
    itsystem_json "${bvn}" "${titel}" "${organisation}"
    echo DRY_RUN CHECK
)
