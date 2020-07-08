#!/bin/bash
# Du vil tilføje en klasse til facetten med den brugervendte nøgle 'time_planning'
# Check selv at klassen med den bvn ikke findes i forvejen
# Kald nu dette script med:
#
# scope=TEXT bvn=noegle title="mytitle" facet=time_planning bash moxklas.sh
#
# tilføj en parameter for at få et dry_run check
#
# scriptet finder selv uuider for facet og organisation



facet_class_json(){
	facet=$1
	bvn=$2
	titel=$3
	organisation=$4
	scope=$5
	(cat << EOKLASS
	{ 
	    "attributter": { 
		"klasseegenskaber": [ 
		    {
		    "brugervendtnoegle": "${bvn}", 
		    "titel": "${titel}", 
		    "omfang": "${scope}", 
		    "virkning": { 
			"from": "1930-01-01 12:02:32", 
			"to": "infinity"
		    } 
		    }
		] 
	    }, 
	    "tilstande": { 
		"klassepubliceret": [{ 
		    "publiceret": "Publiceret", 
		    "virkning": { 
			"from": "1930-01-01 12:02:32",
			"to": "infinity"
		    } 
		}
		] 
	    },
	    "relationer": { 
		"ansvarlig": [
		{ 
		    "uuid": "${organisation}", 
		    "virkning": { 
			"from": "1930-01-01 12:02:32",  
			"to": "infinity"
		    },
		    "objekttype": "organisation"
		}
		],
	   "facet": [ 
		{ 
		    "uuid": "${facet}", 
		    "virkning": { 
			"from": "1930-01-01 12:02:32", 
			"to": "infinity" 
		    }
		}
		] 
	    }

	}
EOKLASS
)}

opret(){
    curl --header "Content-Type: application/json" -X POST http://localhost:8080/klassifikation/klasse -d @-
}

facet_list(){
    echo facet bvn: id
    all_facets=$(curl http://localhost:8080/klassifikation/facet\?bvn=% | jq -r .results[][])
    for facet in $all_facets; do
        curl "http://localhost:8080/klassifikation/facet/${facet}" | jq -r '.["'"${facet}"'"][] | .registreringer[].attributter[][].brugervendtnoegle + " " + .id' 2>/dev/null
    done
}

facet=$(facet_list | grep "${facet}" | cut -d" " -f2)
#bvn="$2"
#titel="$3"
#dry_run="$4"
dry_run="$1"
organisation=$(curl http://localhost:8080/organisation/organisation?bvn=% | jq -r .results[][])


existing=$(curl "http://localhost:8080/klassifikation/klasse?bvn=${bvn}" | jq -r .results[][])
if [ -n "${existing}" ]; then
    echo "Brugervendt nøgle eksisterer"
    exit 1
fi

if [ -n "${facet}" ] && [ -n "${bvn}" ] && [ -n "${titel}" ] && [ -n "${scope}" ] && [ -z "${dry_run}" ]; then
    facet_class_json "${facet}" "${bvn}" "${titel}" "${organisation}" "${scope}" | opret
else
    echo DRY_RUN CHECK: Du har angivet:

    echo "bvn: $bvn"
    echo "facet: $facet"
    echo "titel: $titel"
    echo "scope: $scope"

    echo DRY_RUN CHECK - facetlist
    facet_list
    echo DRY_RUN CHECK - postdata
    facet_class_json "${facet}" "${bvn}" "${titel}" "${organisation}" "${scope}"
    echo DRY_RUN CHECK
fi
