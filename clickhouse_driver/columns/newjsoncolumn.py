from .base import Column
from .stringcolumn import String
from ..reader import read_binary_uint8, read_binary_bytes_fixed_len, read_binary_str
from ..util.compat import json
from ..writer import write_binary_uint8, write_binary_uint64


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
        # Skip padding.
        read_binary_bytes_fixed_len(buf, 9)

        # Read JSON paths.
        paths_count = read_binary_uint8(buf)
        paths = {}
        for i in range(paths_count):
            paths[read_binary_str(buf)] = None

        # Read value specs.
        read_binary_uint8(buf)
        for i in paths:
            read_binary_bytes_fixed_len(buf, 9)
            paths[i] = read_binary_str(buf)
            read_binary_bytes_fixed_len(buf, 9)

        # Read values.
        for path, spec in paths.items():
            col = self.column_by_spec_getter(spec)
            paths[path] = col.read_items(1, buf)[0]

        read_binary_bytes_fixed_len(buf, 8)

        result = [paths]
        return result

    def write_items(self, items, buf):
        # Convert all items to dictionaries.
        items = [x if not isinstance(x, str) else json.loads(x) for x in items]

        # Write padding bytes.
        buf.write(b"\x00" * 7)

        # Convert items into desired format and write them.
        paths = self._serialize_json(items)
        write_binary_uint8(len(paths), buf)
        self.string_column.write_items(paths.keys(), buf)

        # Write values specs.
        for col in list(paths.values()):
            buf.write(b"\x02" + b"\x00" * 7)
            write_binary_uint8(len(col), buf)
            self.string_column.write_items(col.keys(), buf)
            buf.write(b"\x00" * 8)

        # Write values.
        for jcol in paths.values():
            buf.write(self._get_row_posititons(jcol, len(items)))
            for spec in jcol:
                if spec.startswith("Array"):
                    for item in jcol[spec]["values"]:
                        write_binary_uint64(len(item), buf)
                        buf.write(b"\x00" * 2)
                        self.string_column.write_items(item, buf)
                else:
                    col = self.column_by_spec_getter(spec)
                    col.write_items(jcol[spec]["values"], buf)

        # Write final padding.
        buf.write(b"\x00" * len(items) * 8)

    def _get_json_value_spec(self, val):
        if isinstance(val, int) and not isinstance(val, bool):
            return "Int64"
        elif isinstance(val, float):
            return "Float64"
        elif isinstance(val, str):
            return "String"
        elif isinstance(val, bool):
            return "Bool"
        elif isinstance(val, list):
            return "Array(Nullable(String))"

    def _get_row_posititons(self, col, row_count):
        result = [255] * row_count
        count = 0
        for spec in col:
            if count == len(col) - 1:
                count += 1
            for pos in col[spec]["positions"]:
                result[pos] = count
            count += 1
        return bytes(result)

    def _serialize_json_item(self, obj, result={}, row_count=0):
        if isinstance(obj, dict):
            for k in obj:
                obj_res = self._serialize_json_item(obj[k])
                for obj_k in obj_res:
                    if f"{k}.{obj_k}" not in result:
                        result[f"{k}.{obj_k}"] = {}
                    spec = self._get_json_value_spec(obj_res[obj_k])
                    if spec not in result[f"{k}.{obj_k}"]:
                        result[f"{k}.{obj_k}"][spec] = {
                            "values": [], "positions": []}
                    result[f"{k}.{obj_k}"][spec]["values"].append(
                        obj_res[obj_k])
                    result[f"{k}.{obj_k}"][spec]["positions"].append(row_count)
            return result
        else:
            return {"": obj}

    def _serialize_json(self, items):
        result = {}
        for row, obj in enumerate(items):
            result = self._serialize_json_item(obj, result, row)
        for k in list(result.keys()):
            result[k[:-1]] = dict(sorted(result[k].items()))
            del result[k]
        result = dict(sorted(result.items()))
        return result


def create_newjson_column(spec, column_by_spec_getter, column_options):
    return NewJsonColumn(column_by_spec_getter, **column_options)
