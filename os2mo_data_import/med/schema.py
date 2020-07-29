SCHEMA = {
    (False, "facet"): {
        "type": "object",
        "id": "http://github.com/magenta-aps/mox",
        "definitions": {
            "empty_string": {"type": "string", "pattern": "^$"},
            "urn": {"type": "string", "pattern": "^urn:."},
            "virkning": {
                "type": "object",
                "properties": {
                    "from_included": {"type": "boolean"},
                    "to_included": {"type": "boolean"},
                    "aktoerref": {"$ref": "#/definitions/uuid"},
                    "from": {"type": "string"},
                    "to": {"type": "string"},
                    "aktoertypekode": {"type": "string"},
                    "notetekst": {"type": "string"},
                },
                "additionalProperties": False,
                "required": ["from", "to"],
            },
            "offentlighedundtaget": {
                "type": "object",
                "properties": {
                    "alternativtitel": {"type": "string"},
                    "hjemmel": {"type": "string"},
                },
                "additionalProperties": False,
                "required": ["alternativtitel", "hjemmel"],
            },
            "uuid": {
                "type": "string",
                "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$",
            },
        },
        "properties": {
            "relationer": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "redaktoerer": {
                        "type": "array",
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                    "ansvarlig": {
                        "type": "array",
                        "maxItems": 1,
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                    "ejer": {
                        "type": "array",
                        "maxItems": 1,
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                    "facettilhoerer": {
                        "type": "array",
                        "maxItems": 1,
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                },
            },
            "note": {"type": "string"},
            "attributter": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "facetegenskaber": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "retskilde": {"type": "string"},
                                "brugervendtnoegle": {"type": "string"},
                                "supplement": {"type": "string"},
                                "opbygning": {"type": "string"},
                                "integrationsdata": {"type": "string"},
                                "virkning": {"$ref": "#/definitions/virkning"},
                                "ophavsret": {"type": "string"},
                                "plan": {"type": "string"},
                                "beskrivelse": {"type": "string"},
                            },
                            "additionalProperties": False,
                            "required": ["virkning"],
                        },
                    }
                },
            },
            "tilstande": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "facetpubliceret": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "publiceret": {
                                    "type": "string",
                                    "enum": ["Publiceret", "IkkePubliceret"],
                                },
                                "virkning": {"$ref": "#/definitions/virkning"},
                            },
                            "additionalProperties": False,
                            "required": ["publiceret", "virkning"],
                        },
                    }
                },
            },
            "livscyklus": {"type": "string"},
        },
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": False,
    },
    (True, "facet"): {
        "type": "object",
        "id": "http://github.com/magenta-aps/mox",
        "definitions": {
            "empty_string": {"type": "string", "pattern": "^$"},
            "urn": {"type": "string", "pattern": "^urn:."},
            "virkning": {
                "type": "object",
                "properties": {
                    "from_included": {"type": "boolean"},
                    "to_included": {"type": "boolean"},
                    "aktoerref": {"$ref": "#/definitions/uuid"},
                    "from": {"type": "string"},
                    "to": {"type": "string"},
                    "aktoertypekode": {"type": "string"},
                    "notetekst": {"type": "string"},
                },
                "additionalProperties": False,
                "required": ["from", "to"],
            },
            "offentlighedundtaget": {
                "type": "object",
                "properties": {
                    "alternativtitel": {"type": "string"},
                    "hjemmel": {"type": "string"},
                },
                "additionalProperties": False,
                "required": ["alternativtitel", "hjemmel"],
            },
            "uuid": {
                "type": "string",
                "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$",
            },
        },
        "properties": {
            "relationer": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "redaktoerer": {
                        "type": "array",
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                    "ansvarlig": {
                        "type": "array",
                        "maxItems": 1,
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                    "ejer": {
                        "type": "array",
                        "maxItems": 1,
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                    "facettilhoerer": {
                        "type": "array",
                        "maxItems": 1,
                        "items": {
                            "oneOf": [
                                {
                                    "type": "object",
                                    "required": ["uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "uuid": {"$ref": "#/definitions/uuid"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "objekttype": {"type": "string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/urn"},
                                    },
                                },
                                {
                                    "type": "object",
                                    "required": ["urn", "uuid", "virkning"],
                                    "additionalProperties": False,
                                    "properties": {
                                        "uuid": {"$ref": "#/definitions/empty_string"},
                                        "virkning": {"$ref": "#/definitions/virkning"},
                                        "urn": {"$ref": "#/definitions/empty_string"},
                                    },
                                },
                            ]
                        },
                    },
                },
            },
            "note": {"type": "string"},
            "attributter": {
                "type": "object",
                "properties": {
                    "facetegenskaber": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "retskilde": {"type": "string"},
                                "brugervendtnoegle": {"type": "string"},
                                "supplement": {"type": "string"},
                                "opbygning": {"type": "string"},
                                "integrationsdata": {"type": "string"},
                                "virkning": {"$ref": "#/definitions/virkning"},
                                "ophavsret": {"type": "string"},
                                "plan": {"type": "string"},
                                "beskrivelse": {"type": "string"},
                            },
                            "additionalProperties": False,
                            "required": ["brugervendtnoegle", "virkning"],
                        },
                    }
                },
                "additionalProperties": False,
                "required": ["facetegenskaber"],
            },
            "tilstande": {
                "type": "object",
                "properties": {
                    "facetpubliceret": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "publiceret": {
                                    "type": "string",
                                    "enum": ["Publiceret", "IkkePubliceret"],
                                },
                                "virkning": {"$ref": "#/definitions/virkning"},
                            },
                            "additionalProperties": False,
                            "required": ["publiceret", "virkning"],
                        },
                    }
                },
                "additionalProperties": False,
                "required": ["facetpubliceret"],
            },
            "livscyklus": {"type": "string"},
        },
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": False,
        "required": ["attributter", "tilstande"],
    },
}

from jsonschema import validate
def validate_payload(instance, obj, create=True):
    schema = SCHEMA[(create, obj)]
    validate(instance=instance, schema=schema)
