"""Load OpenAPI YAML with JSON-compatible booleans and timestamp strings."""
import re
import json
import yaml


class OpenApiLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)):
    pass


OpenApiLoader.yaml_implicit_resolvers = {
    key: [(tag, pattern) for tag, pattern in values
          if tag not in ("tag:yaml.org,2002:bool", "tag:yaml.org,2002:timestamp")]
    for key, values in OpenApiLoader.yaml_implicit_resolvers.items()
}
OpenApiLoader.add_implicit_resolver("tag:yaml.org,2002:bool", re.compile(r"^(?:true|false)$", re.IGNORECASE), list("tTfF"))


def load(text):
    try:
        return json.loads(text)
    except ValueError:
        pass
    return yaml.load(text, Loader=OpenApiLoader)


def dumps(document):
    try:
        return json.dumps(document, default=str)
    except ValueError as error:
        if "Circular reference" not in str(error):
            raise
    # YAML merge aliases can copy a schema while retaining its recursive properties/union
    # containers. Reuse an equivalent declared component, preserving the entire constraint
    # set instead of truncating recursion or replacing a properties map with an invalid $ref.
    schemas = document.get("components", {}).get("schemas", {})
    annotations = {"description", "title", "example", "examples", "default", "deprecated", "externalDocs"}

    def signature(schema):
        if not isinstance(schema, dict): return None
        def identity(value):
            if isinstance(value, list) and all(not isinstance(v, (dict, list)) for v in value):
                return ("values", tuple(value))
            if isinstance(value, (dict, list)): return ("identity", id(value))
            return ("scalar", value)
        return tuple(sorted((key, identity(value)) for key, value in schema.items() if key not in annotations))

    components = {}
    for name, schema in schemas.items():
        if isinstance(schema, dict): components.setdefault(signature(schema), name)
    active = set()

    def convert(value, context="ordinary", path=(), own_component=None):
        if not isinstance(value, (dict, list)): return value
        if context == "schema":
            name = components.get(signature(value))
            if name is not None and name != own_component:
                reference = {"$ref": "#/components/schemas/" + name.replace("~", "~0").replace("/", "~1")}
                reference.update({k: convert(v, "payload", path + (k,)) for k, v in value.items() if k in annotations})
                return reference
        if id(value) in active:
            raise ValueError("Recursive YAML alias has no equivalent component schema at " + "/".join(map(str, path)))
        active.add(id(value))
        try:
            if isinstance(value, list):
                return [convert(v, "schema" if context == "schema_array" else context, path + (i,)) for i, v in enumerate(value)]
            result = {}
            for key, child in value.items():
                child_context = "payload" if context == "payload" or key in annotations else "ordinary"
                own = None
                if context == "schema_map":
                    child_context = "schema"
                    if path == ("components", "schemas"): own = key
                elif context != "payload":
                    if path == ("components",) and key == "schemas": child_context = "schema_map"
                    elif key == "schema": child_context = "schema"
                    elif context == "schema":
                        if key in ("properties", "patternProperties", "$defs", "definitions", "dependentSchemas"): child_context = "schema_map"
                        elif key in ("allOf", "anyOf", "oneOf", "prefixItems"): child_context = "schema_array"
                        elif key in ("items", "additionalProperties", "not", "if", "then", "else", "contains"): child_context = "schema"
                result[key] = convert(child, child_context, path + (key,), own)
            return result
        finally:
            active.remove(id(value))

    return json.dumps(convert(document), default=str)
