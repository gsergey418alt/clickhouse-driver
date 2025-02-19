from .base import Column
from .stringcolumn import String
from ..reader import read_binary_uint8, read_binary_str
from ..util.compat import json
from ..writer import write_binary_uint8


class NewJsonColumn(Column):
    py_types = (dict, )

    # No NULL value actually
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.string_column = String(**kwargs)
        super(NewJsonColumn, self).__init__(**kwargs)

    def write_state_prefix(self, buf):
        # Read in binary format.
        # Write in text format.
        write_binary_uint8(2, buf)

    def read_items(self, n_items, buf):
        pass

    def write_items(self, items, buf):

        # Convert all items to dictionaries.
        items = [x if not isinstance(x, str) else json.loads(x) for x in items]

        # Write padding bytes.
        buf.write(b"\x00" * 7)

        # Convert items into desired format and write them.
        paths = self.serialize_json(items[0])
        write_binary_uint8(len(paths), buf)
        self.string_column.write_items(paths.keys(), buf)

        # Write values types.
        buf.write(b"\x02" + b"\x00" * 7 + b"\x01")
        for val in list(paths.values())[:-1]:
            self.string_column.write_items([self._get_json_value_spec(val)], buf)
            buf.write(b"\x00" * 7 + b"\x02" + b"\x00" * 7 + b"\x01")
        self.string_column.write_items([self._get_json_value_spec(list(paths.values())[-1])], buf)
        buf.write(b"\x00" * 8 + b"\x01")

        # Write values
        for val in paths.values():
            spec = self._get_json_value_spec(val)
            col = self.column_by_spec_getter(spec)
            col.write_items([val], buf)
            buf.write(b"\x00")
        buf.write(b"\x01" + b"\x00" * 6)

    def _get_json_value_spec(self, val):
        if isinstance(val, int):
            return "Int64"
        elif isinstance(val, float):
            return "Float64"
        elif isinstance(val, str):
            return "String"
        elif isinstance(val, bool):
            return "Bool"
        elif isinstance(val, None):
            return "String"
        elif isinstance(val, list):
            return "Tuple(Nullable(String))"
    
    def _serialize_json_item(self, obj):
        if isinstance(obj, dict):
            result = {}
            for k in obj:
                obj_res = self._serialize_json_item(obj[k])
                for obj_k in obj_res:
                    result[f"{k}.{obj_k}"] = obj_res[obj_k]
            return result
        else:
            return {"": obj}

    def serialize_json(self, obj):
        res = self._serialize_json_item(obj)
        for k in list(res.keys()):
            res[k[:-1]] = res[k]
            del res[k]
        res = dict(sorted(res.items()))
        return res


def create_newjson_column(spec, column_by_spec_getter, column_options):
    return NewJsonColumn(column_by_spec_getter, **column_options)
