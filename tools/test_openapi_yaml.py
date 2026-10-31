import json
import unittest
from openapi_yaml import load, dumps


class OpenApiYamlTests(unittest.TestCase):
    def test_json_compatible_scalars_and_surrogate_pairs(self):
        document = load("properties: {on: {type: string}, off: {type: string}}\nexample: 2026-01-02\nnullable: true")
        self.assertEqual(set(document["properties"]), {"on", "off"})
        self.assertEqual(document["example"], "2026-01-02")
        self.assertIs(document["nullable"], True)
        self.assertEqual(load(r'{"example":"\ud83d\ude00"}')["example"], "😀")

    def test_recursive_property_alias_uses_equivalent_component(self):
        document = load("""
components:
  schemas:
    Node:
      type: object
      properties: &fields
        name: {type: string}
        child:
          description: A child node
          type: object
          properties: *fields
      example: {schema: {type: object}}
""")
        converted = json.loads(dumps(document))
        node = converted["components"]["schemas"]["Node"]
        self.assertEqual(node["properties"]["child"]["$ref"], "#/components/schemas/Node")
        self.assertEqual(node["properties"]["child"]["description"], "A child node")
        self.assertEqual(node["example"], {"schema": {"type": "object"}})

    def test_different_constraints_are_not_discarded_to_break_recursion(self):
        document = load("""
components:
  schemas:
    Node:
      type: object
      properties: &fields
        child:
          type: object
          required: [extra]
          properties: *fields
""")
        with self.assertRaisesRegex(ValueError, "no equivalent component"):
            dumps(document)

    def test_recursive_payload_is_not_rewritten_as_a_schema_reference(self):
        document = load("components: {schemas: {Node: {type: object, example: &example {child: *example}}}}")
        with self.assertRaisesRegex(ValueError, "no equivalent component"):
            dumps(document)


if __name__ == "__main__":
    unittest.main()
