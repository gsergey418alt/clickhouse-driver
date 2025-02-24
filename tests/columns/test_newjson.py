import json

from tests.testcase import BaseTestCase


class NewJSONTestCase(BaseTestCase):
    required_server_version = (22, 3, 2)

    def client_kwargs(self, version):
        return {'settings': {'enable_json_type': True}}

    def cli_client_kwargs(self):
        return {'enable_json_type': 1}

    def test_simple(self):
        rv = self.client.execute("SELECT '{\"bb\": {\"cc\": [255, 1]}}'::JSON")
        self.assertEqual(rv, [({'bb': {'cc': [255, 1]}},)])

    def test_from_table(self):
        with self.create_table('a JSON'):
            data = [
                ({},),
                ({
                    "foo": "bar",
                    "bar": "baz"
                },),
                ({
                    "baz": "qux",
                    "foo": 4919
                },),
                ({"qux": "quux"},),
                ({"foo": "AAAA"},),
                ({"qux": 14099},),
                ({"foo": [1, 0.2, "bar", "baz", False]},),
                ({"foo": 0.1337},),
                ({"foo": False},),
                ({"bar": 1337},),
                ({"bar": 0.999},),
                ({"quux": 1000},),
                ({"quux": 2000},),
                ({"alice": 0.432},),
                ({"bob": 0.991},),
                ({"boolean": True},),
                ({"string": "A quick brown fox jumps over the lazy dog."},),
                ({"nested": {
                    "number": 4141,
                    "string": "Hello, World!",
                    "double-nested": {
                        "foo": "bar",
                        "no.escaping": "1337",
                        "triple-nested": {
                            "foo": "bar"
                        },
                        "numbers": [1, 2, 3],
                        "floats": [0.1, 0.2, 4]
                    }
                }
                },)]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '{}\n'
                '{"bar":"baz","foo":"bar"}\n'
                '{"baz":"qux","foo":"4919"}\n'
                '{"qux":"quux"}\n'
                '{"foo":"AAAA"}\n'
                '{"qux":"14099"}\n'
                '{"foo":["1","0.2","bar","baz","false"]}\n'
                '{"foo":0.1337}\n'
                '{"foo":false}\n'
                '{"bar":"1337"}\n'
                '{"bar":0.999}\n'
                '{"quux":"1000"}\n'
                '{"quux":"2000"}\n'
                '{"alice":0.432}\n'
                '{"bob":0.991}\n'
                '{"boolean":true}\n'
                '{"string":"A quick brown fox jumps over the lazy dog."}\n'
                '{"nested":{"double-nested":{"floats":[0.1,0.2,4],"foo":"bar","no":{"escaping":"1337"},"numbers":["1","2","3"],"triple-nested":{"foo":"bar"}},"number":"4141","string":"Hello, World!"}}\n'
            )
            inserted = self.client.execute(query)
            data_with_all_keys = [
                ({},),
                (
                    {
                        "bar": "baz",
                        "foo": "bar"
                    },),
                (
                    {
                        "baz": "qux",
                        "foo": 4919
                    },),
                (
                    {
                        "qux": "quux"
                    },),
                (
                    {
                        "foo": "AAAA"
                    },),
                (
                    {
                        "qux": 14099
                    },),
                (
                    {
                        "foo": (
                            "1",
                            "0.2",
                            "bar",
                            "baz",
                            "false"
                        )
                    },),
                (
                    {
                        "foo": 0.1337
                    },),
                (
                    {
                        "foo": False
                    },),
                (
                    {
                        "bar": 1337
                    },),
                (
                    {
                        "bar": 0.999
                    },),
                (
                    {
                        "quux": 1000
                    },),
                (
                    {
                        "quux": 2000
                    },),
                (
                    {
                        "alice": 0.432
                    },),
                (
                    {
                        "bob": 0.991
                    },),
                (
                    {
                        "boolean": True
                    },),
                (
                    {
                        "string": "A quick brown fox jumps over the lazy dog."
                    },),
                (
                    {
                        "nested": {
                            "double-nested": {
                                "floats": (
                                    0.1,
                                    0.2,
                                    4.0
                                ),
                                "foo": "bar",
                                "no": {
                                    "escaping": "1337"
                                },
                                "numbers": (
                                    1,
                                    2,
                                    3
                                ),
                                "triple-nested": {
                                    "foo": "bar"
                                }
                            },
                            "number": 4141,
                            "string": "Hello, World!"
                        }
                    },)
            ]
            self.assertEqual(inserted, data_with_all_keys)

    def test_insert_json_strings(self):
        with self.create_table('a JSON'):
            data = [
                (json.dumps({'i-am': 'dumped json'}),),
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '{"i-am":"dumped json"}\n'
            )
            inserted = self.client.execute(query)
            data_with_all_keys = [
                ({'i-am': 'dumped json'},)
            ]
            self.assertEqual(inserted, data_with_all_keys)

    def test_json_as_named_tuple(self):
        query = 'SELECT * FROM test'

        with self.create_table('a JSON'):
            data = [
                ({'key': 'value'}, ),
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
