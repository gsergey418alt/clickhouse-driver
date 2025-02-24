from .base import Column
from .stringcolumn import String
from ..reader import read_binary_uint8, read_binary_bytes_fixed_len, read_binary_str, read_binary_str_fixed_len, read_binary_uint64
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
            paths[read_binary_str(buf)] = {}

        # Read value specs.
        read_binary_uint8(buf)
        for path in paths.values():
            read_binary_bytes_fixed_len(buf, 8)

            # ClickHouse client repeats the spec count bytes twice if
            # there are more than two different specs for a single column.
            spec_count = read_binary_uint8(buf)
            next_byte = read_binary_uint8(buf)
            next_next_byte = read_binary_uint8(buf)
            if chr(next_next_byte).isalnum():
                spec = chr(next_next_byte) + \
                    read_binary_str_fixed_len(buf, next_byte - 1)
                path[spec] = {
                    "values": [], "positions": []}
            else:
                if spec_count != next_byte:
                    raise Exception(
                        f"Parsing error: spec length verficiation byte invalid: {spec_count} != {next_byte}.")
                spec = read_binary_str_fixed_len(buf, next_next_byte)
                path[spec] = {
                    "values": [], "positions": []}

            for i in range(1, spec_count):
                spec = read_binary_str(buf)
                path[spec] = {
                    "values": [], "positions": []}

            read_binary_bytes_fixed_len(buf, 8)

        # Read values.
        for path in paths.values():
            specs = []
            for i in range(n_items):
                spec_number = read_binary_uint8(buf)
                if spec_number < 255:
                    if spec_number > len(path) - 1 and not (len(path) <= 2 and "String" in path.values()):
                        spec_number -= 1
                    spec = list(path.keys())[spec_number]
                    specs.append(spec)
                    path[spec]["positions"].append(i)

            specs = sorted(specs)
            for spec in specs:
                if spec.startswith("Array"):
                    if len(path[spec]["values"]) > 0:
                        continue
                    bound = read_binary_uint64(buf)
                    bounds = [bound]
                    while True:
                        bound = read_binary_uint8(buf)
                        if bound == 0:
                            read_binary_bytes_fixed_len(buf, bounds[-1] - 1)
                            break
                        else:
                            for i in range(1, 8):
                                bound += read_binary_uint8(buf) << (8 * i)
                            bounds.append(bound)

                    col = self.column_by_spec_getter(spec[6:-1])
                    prev_bound = 0
                    for bound in bounds:
                        path[spec]["values"].append(col.read_items(bound - prev_bound, buf))
                        prev_bound = bound
                else:
                    col = self.column_by_spec_getter(spec)
                    path[spec]["values"] += col.read_items(1, buf)

        read_binary_bytes_fixed_len(buf, 8 * n_items)

        return self._fold_json(n_items, paths)

    def write_items(self, items, buf):
        # Convert all items to dictionaries.
        items = [x if not isinstance(x, str) else json.loads(x) for x in items]

        # Write padding bytes.
        buf.write(b"\x00" * 7)

        # Convert items into desired format and write them.
        paths = self._unfold_json(items)
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
                    bound = 0
                    for item in jcol[spec]["values"]:
                        bound += len(item)
                        write_binary_uint64(bound, buf)
                        
                    buf.write(b"\x00" * bound)

                    array_type = spec[6:-1]
                    for item in jcol[spec]["values"]:
                        insert = []
                        if array_type == "Nullable(String)":
                            for elem in item:
                                if isinstance(elem, str):
                                    insert.append(elem)
                                elif isinstance(elem, bool):
                                    insert.append(str(elem).lower())
                                else:
                                    insert.append(str(elem))
                        else:
                            insert = item
                        col = self.column_by_spec_getter(spec[6:-1])
                        col.write_items(insert, buf)
                else:
                    col = self.column_by_spec_getter(spec)
                    col.write_items(jcol[spec]["values"], buf)

        # Write final padding.
        buf.write(b"\x00" * len(items) * 8)

    def _get_json_value_spec(self, val):
        """
        Returns a ClickHouse spec for a JSON data type.
        """
        if isinstance(val, int) and not isinstance(val, bool):
            return "Int64"
        elif isinstance(val, float):
            return "Float64"
        elif isinstance(val, str):
            return "String"
        elif isinstance(val, bool):
            return "Bool"
        elif isinstance(val, list):
            val_types = []
            for item in val:
                t = type(item)
                if t not in val_types:
                    val_types.append(t)
            if dict in val_types or list in val_types:
                return "Array(Nullable(String))"
            else:
                if str in val_types:
                    return "Array(Nullable(String))"
                elif float in val_types:
                    if bool not in val_types:
                        return "Array(Nullable(Float64))"
                    else:
                        return "Array(Nullable(String))"
                else:
                    return "Array(Nullable(Int64))"

    def _get_row_posititons(self, col, row_count):
        """
        Returns bytes corresponding to the position of specs between records.
        """
        result = [255] * row_count
        count = 0
        for spec in col:
            if count == len(col) - 1 and not (len(col) <= 2 and not "String" in col):
                count += 1
            for pos in col[spec]["positions"]:
                result[pos] = count
            count += 1
        return bytes(result)

    def _normalize_json(self, obj,):
        """
        Deals with converting a nested dictionary to a dictionary of paths with depth one.
        """
        if isinstance(obj, dict):
            result = {}
            for k in obj:
                if obj[k] is not None:
                    obj_res = self._normalize_json(obj[k])
                    for obj_k in obj_res:
                        result[f"{k}.{obj_k}"] = obj_res[obj_k]
            return result
        else:
            return {"": obj}

    def _unfold_json_item(self, obj, result={}, row_count=0):
        """
        Converts a single record into an intermeditary format stored in result.
        """
        for k in obj:
            if obj[k] is not None:
                obj_res = self._normalize_json(obj[k])
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

    def _unfold_json(self, items):
        """
        Converts the passed dictionary into an intermediary format.
        """
        result = {}
        for row, obj in enumerate(items):
            result = self._unfold_json_item(obj, result, row)
        for k in list(result.keys()):
            result[k[:-1]] = dict(sorted(result[k].items()))
            del result[k]
        result = dict(sorted(result.items()))
        return result
    
    def _denormalize_json(self, obj):
        """
        Converts a dictionary of paths with depth one to a nested dictionary.
        """
        keys = list(obj.keys())
        for key in keys:
            split_key = key.split(".")
            if len(split_key) > 1:
                parent = obj
                for part in split_key[:-1]:
                    if part not in parent:
                        parent[part] = {}
                    parent = parent[part]
                parent[split_key[-1]] = obj[key]
                del obj[key]

    def _fold_json(self, n_items, obj):
        """
        Converts an intermediary record back to a list of rows
        """
        result = [{} for _ in range(n_items)]

        for key, item in obj.items():
            for spec in item.values():
                for i in range(len(spec["values"])):
                    result[spec["positions"][i]][key] = spec["values"][i]

        [self._denormalize_json(item) for item in result]
        return result


def create_newjson_column(spec, column_by_spec_getter, column_options):
    return NewJsonColumn(column_by_spec_getter, **column_options)
