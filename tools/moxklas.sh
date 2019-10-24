#!/bin/bash
# Du vil tilføje en klasse til facetten med den brugervendte nøgle 'time_planning'
# Check selv at klassen med den bvn ikke findes i forvejen
# Kald nu dette script med:
#
# bash moxklas.sh "time_planning" "bvn_bvn" "This is the title"
#
# tilføj en ekstra parameter for at få et dry_run check
#
# scriptet finder selv uuider for facet og organisation



facet_class_json(){
	facet=$1
	bvn=$2
	titel=$3
	organisation=$4
	(cat << EOKLASS
	{ 
	    "attributter": { 
		"klasseegenskaber": [ 
		    {
		    "brugervendtnoegle": "${bvn}", 
		    "titel": "${titel}", 
		    "omfang": "TEXT", 
		    "virkning": { 
			"from": "1900-01-01 12:02:32", 
			"to": "infinity"
		    } 
		    }
		] 
	    }, 
	    "tilstande": { 
		"klassepubliceret": [{ 
		    "publiceret": "Publiceret", 
		    "virkning": { 
			"from": "1900-01-01 12:02:32",
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
			"from": "1900-01-01 12:02:32",  
			"to": "infinity"
		    },
		    "objekttype": "organisation"
		}
		],
	   "facet": [ 
		{ 
		    "uuid": "${facet}", 
		    "virkning": { 
			"from": "1900-01-01 12:02:32", 
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
    (
    all_facets=$(curl http://localhost:8080/klassifikation/facet\?bvn=\% | jq -r .results[][])
    for facet in $all_facets; do
        curl http://localhost:8080/klassifikation/facet/${facet} | jq -r '.["'${facet}'"][] | .registreringer[].attributter[][].brugervendtnoegle + " " + .id'
    done
    ) 2>/dev/null
}

facet=$(facet_list | grep $1 | cut -d" " -f2)
bvn="$2"
titel="$3"
dry_run="$4"
organisation=$(curl http://localhost:8080/organisation/organisation?bvn=% | jq -r .results[][])


existing=$(curl http://localhost:8080/klassifikation/klasse?bvn=${bvn} | jq -r .results[][])
if [ -n "${existing}" ]; then
    echo "Brugervendt nøgle eksisterer"
    exit 1
fi

[ -n "${facet}" -a -n "${bvn}" -a -n "${titel}" -a -z "${dry_run}" ] && (
    facet_class_json "${facet}" "${bvn}" "${titel}" "${organisation}" | opret
) || (
    echo DRY_RUN CHECK - facetlist
    facet_list
    echo DRY_RUN CHECK - postdata
    facet_class_json "${facet}" "${bvn}" "${titel}" "${organisation}"
    echo DRY_RUN CHECK
)
