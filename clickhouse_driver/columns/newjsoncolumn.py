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
        paths = self._read_paths(buf)
        if paths is None:
            shared_paths = self._read_shared_paths(buf)
            self._read_shared_values(buf, shared_paths)
            return []
        self._read_specs(buf, paths)
        self._read_values(buf, paths, n_items)

        return self._fold_json(n_items, paths)

    def _read_paths(self, buf):
        """
        Read JSON paths.
        """
        read_binary_bytes_fixed_len(buf, 9)

        paths_count = read_binary_uint8(buf)
        if paths_count == 0:
            print("Warning: shared path JSON deserialization not implemented, skipping shared paths.")
            return
        paths = {}
        for i in range(paths_count):
            strlen = read_binary_uint8(buf)
            col = read_binary_str_fixed_len(buf, strlen)
            paths[col] = {}

        return paths

    def _read_specs(self, buf, paths):
        """
        Read value specs.
        """
        for col in paths.values():
            read_binary_bytes_fixed_len(buf, 8)

            start = 0
            # ClickHouse client repeats the spec count bytes twice if
            # there are more than two different specs for a single column.
            spec_count = read_binary_uint8(buf)
            next_byte = read_binary_uint8(buf)
            if next_byte != spec_count:
                spec = read_binary_str_fixed_len(buf, next_byte)
                col[spec] = {
                    "values": [], "positions": []}
                start = 1

            for i in range(start, spec_count):
                spec = read_binary_str(buf)
                col[spec] = {
                    "values": [], "positions": []}

            read_binary_bytes_fixed_len(buf, 8)

            for spec in col:
                if spec.startswith("Tuple") and "JSON" in spec:
                    self._read_complex_tuple_header(buf, col, spec)
                elif spec.startswith("Array") and "JSON" in spec:
                    self._read_complex_array_header(buf, col, spec)

    def _read_complex_tuple_header(self, buf, col, spec):
        """
        Read header for JSON objects inside a tuple.
        """
        col[spec]["tuple_header"] = []
        for i, subspec in enumerate(spec[6:-2].split("), ")):
            if subspec.startswith("JSON"):
                paths = self._read_paths(buf)
                if paths is None:
                    col[spec]["tuple_header"] += [None for _ in range(len(spec[6:-2].split("), ")) - i)]
                    return
                self._read_specs(buf, paths)
                col[spec]["tuple_header"].append(paths)
            else:
                col[spec]["tuple_header"].append(None)

    def _read_complex_array_header(self, buf, col, spec):
        """
        Read header for JSON objects inside an array.
        """
        paths = self._read_paths(buf)
        self._read_specs(buf, paths)
        col[spec]["array_header"] = paths

    def _read_values(self, buf, paths, n_items):
        """
        Read values.
        """
        for col in paths.values():
            specs = self._read_row_positions(buf, col, n_items)

            # Read values of that column.
            for spec in specs:
                if spec.startswith("Array"):
                    if "JSON" in spec:
                        self._read_complex_array_values(buf, col, spec)
                    else:
                        reader = self.column_by_spec_getter(spec)
                        col[spec]["values"] = reader.read_data(
                            len(col[spec]["positions"]), buf)
                elif spec.startswith("Tuple"):
                    if "JSON" in spec:
                        self._read_complex_tuple_values(buf, col, spec)
                    else:
                        reader = self.column_by_spec_getter(spec)
                        col[spec]["values"] += reader.read_items(
                            len(col[spec]["positions"]), buf)
                else:
                    reader = self.column_by_spec_getter(spec)
                    col[spec]["values"] += reader.read_items(1, buf)

        read_binary_bytes_fixed_len(buf, 8 * n_items)

    def _read_complex_tuple_values(self, buf, col, spec):
        """
        Read values in a tuple with nested JSON elements.
        """
        col[spec]["values"] = [[] for _ in range(len(col[spec]["positions"]))]
        for i, subspec in enumerate(spec[6:-2].split("), ")):
            if not subspec.startswith("Array") and not subspec.startswith("Tuple") and not subspec.startswith("JSON"):
                buf.read(len(col[spec]["positions"]))
            for row in col[spec]["values"]:
                if subspec.startswith("JSON"):
                    paths = col[spec]["tuple_header"][i]
                    if paths is None:
                        # Read simplified nested JSON with max_dynamic_types = 0 and max_dynamic_paths = 0.
                        shared_paths = self._read_shared_paths(buf)
                        self._read_shared_values(buf, shared_paths)
                        break
                    self._read_values(buf, paths, len(col[spec]["positions"]))
                    result = self._fold_json(
                        len(col[spec]["positions"]), paths)
                    for pos, item in enumerate(result):
                        col[spec]["values"][pos].append(item)
                    break
                elif subspec.startswith("Array"):
                    reader = self.column_by_spec_getter(
                        subspec + ")")
                    row += reader.read_data(1, buf)
                elif subspec.startswith("Tuple"):
                    reader = self.column_by_spec_getter(
                        subspec[6:])
                    row += reader.read_data(1, buf)
                else:
                    reader = self.column_by_spec_getter(
                        subspec[9:])
                    row += reader.read_data(1, buf)
    
    
    def _read_shared_paths(self, buf):
        """
        Read json paths with max_dynamic_types = 0 and max_dynamic_paths = 0.
        """
        paths_count = read_binary_uint8(buf)
        read_binary_bytes_fixed_len(buf, 7)

        paths = {}
        for i in range(paths_count):
            strlen = read_binary_uint8(buf)
            col = read_binary_str_fixed_len(buf, strlen)
            paths[col] = {}
        
        return paths
    
    def _read_shared_values(self, buf, paths):
        """
        Read json values with max_dynamic_types = 0 and max_dynamic_paths = 0.
        """
        for path in paths:
            content_len = read_binary_uint8(buf)
            paths[path] = self._unmarshal_shared_values(read_binary_bytes_fixed_len(buf, content_len))

    def _unmarshal_shared_values(self, bin):
        """
        Unmarshal json values with max_dynamic_types = 0 and max_dynamic_paths = 0.
        """
        return bin
    
    def _read_complex_array_values(self, buf, col, spec):
        """
        Read values in an array with nested JSON elements.
        """
        bounds = []
        for i in range(len(col[spec]["positions"])):
            bounds.append(read_binary_uint64(buf))
        paths = col[spec]["array_header"]
        self._read_values(buf, paths, bounds[-1])
        result = self._fold_json(
            bounds[-1], paths)
        prev_bound = 0
        for i, bound in enumerate(bounds):
            col[spec]["values"].append(result[prev_bound:bound])
            col[spec]["positions"].append(i)
            prev_bound = bound

    def _read_row_positions(self, buf, col, n_items):
        """
        Read value positions in the record list.
        """
        specs = []
        skip = len(
            col) - len([v for v in col if v.startswith("String") or v.startswith("Tuple")])
        for i in range(n_items):
            spec_number = read_binary_uint8(buf)
            if spec_number < 255:
                if spec_number > skip:
                    spec_number -= 1
                spec = list(col.keys())[spec_number]
                if not (spec.startswith("Array") or spec.startswith("Tuple")) or spec not in specs:
                    specs.append(spec)
                col[spec]["positions"].append(i)

        return sorted(specs)

    def write_items(self, items, buf, depth=0):
        # Convert all items to dictionaries.
        items = [x if not isinstance(x, str) else json.loads(x) for x in items]

        paths = self._unfold_json(items, depth)

        self._write_paths(paths, buf)
        self._write_specs(paths, buf)
        self._write_values(paths, len(items), buf)

    def _write_paths(self, paths, buf):
        """
        Convert items into desired format and write them.
        """
        buf.write(b"\x00" * 7)
        write_binary_uint8(len(paths), buf)
        self.string_column.write_items(paths.keys(), buf)

    def _write_specs(self, paths, buf, depth=0):
        """
        Write values specs.
        """
        for col in paths.values():
            buf.write(b"\x02" + b"\x00" * 7)
            write_binary_uint8(len(col), buf)
            self.string_column.write_items(col.keys(), buf)
            buf.write(b"\x00" * 8)
            for spec in col:
                if spec.startswith("Tuple") and "JSON" in spec:
                    self._write_complex_tuple_header(col, spec, depth+1, buf)
                elif spec.startswith("Array") and "JSON" in spec:
                    self._write_complex_array_header(col, spec, depth+1, buf)

    def _write_values(self, paths, rows, buf, depth=0):
        """
        Write values.
        """
        for col in paths.values():
            buf.write(self._get_row_posititons(col, rows))
            for spec in col:
                if spec.startswith("Array"):
                    if "JSON" in spec:
                        self._write_complex_array_values(
                            col, spec, depth+1, buf)
                    else:
                        insert = self._preprocess_array(
                            col[spec]["values"], spec[6:-1])
                        writer = self.column_by_spec_getter(spec)
                        writer.write_data(insert, buf)
                elif spec.startswith("Tuple"):
                    if "JSON" in spec:
                        self._write_complex_tuple_values(
                            col, spec, depth+1, buf)
                    else:
                        writer = self.column_by_spec_getter(spec)
                        writer.write_items(col[spec]["values"], buf)
                else:
                    writer = self.column_by_spec_getter(spec)
                    writer.write_items(col[spec]["values"], buf)

        # Write final padding.
        buf.write(b"\x00" * rows * 8)

    def _write_complex_tuple_header(self, col, spec, depth, buf):
        """
        Write header for JSON objects inside a tuple.
        """
        for i, subspec in enumerate(spec[6:-2].split("), ")):
            if subspec.startswith("JSON"):
                self.write_state_prefix(buf)
                items = [item[i] for item in col[spec]["values"]]
                paths = self._unfold_json(items, depth=depth)
                self._write_paths(paths, buf)
                self._write_specs(paths, buf, depth=depth)

    def _write_complex_array_header(self, col, spec, depth, buf):
        """
        Write header for JSON objects inside an array.
        """
        self.write_state_prefix(buf)
        items = []
        for item in col[spec]["values"]:
            items += item
        paths = self._unfold_json(items, depth=depth)
        self._write_paths(paths, buf)
        self._write_specs(paths, buf, depth=depth)

    def _write_complex_tuple_values(self, col, spec, depth, buf):
        """
        Write values in a tuple with nested JSON elements.
        """
        for i, subspec in enumerate(spec[6:-2].split("), ")):
            if not subspec.startswith("Array") and not subspec.startswith("Tuple") and not subspec.startswith("JSON"):
                buf.write(b"\x00" * len(col[spec]["values"]))
            for row in col[spec]["values"]:
                if subspec.startswith("JSON"):
                    items = [item[i] for item in col[spec]["values"]]
                    paths = self._unfold_json(items, depth=depth)
                    self._write_values(paths, len(items), buf, depth=depth)
                    break
                elif subspec.startswith("Array"):
                    insert = self._preprocess_array(
                        [row[i]], subspec[6:])
                    writer = self.column_by_spec_getter(
                        subspec + ")")
                    writer.write_data(insert, buf)
                elif subspec.startswith("Tuple"):
                    writer = self.column_by_spec_getter(
                        subspec[6:])
                    writer.write_data([row[i]], buf)
                else:
                    writer = self.column_by_spec_getter(
                        subspec[9:])
                    writer.write_data([row[i]], buf)

    def _write_complex_array_values(self, col, spec, depth, buf):
        """
        Write values in an array with nested JSON elements.
        """
        bound = 0
        for v in col[spec]["values"]:
            bound = bound + len(v)
            write_binary_uint64(bound, buf)
        items = []
        for item in col[spec]["values"]:
            items += item
        paths = self._unfold_json(items, depth=depth)
        self._write_values(paths, len(items), buf, depth=depth)

    def _get_json_value_spec(self, item, depth):
        """
        Returns a ClickHouse spec for a JSON data type.
        """
        if isinstance(item, int) and not isinstance(item, bool):
            return "Int64"
        elif isinstance(item, float):
            return "Float64"
        elif isinstance(item, str):
            return "String"
        elif isinstance(item, bool):
            return "Bool"
        elif isinstance(item, dict):
            return f"JSON(max_dynamic_types={int(2 ** (4 - depth))}, max_dynamic_paths={int(4 ** (4 - depth))})"
        elif isinstance(item, list):
            value_types = []
            for entry in item:
                t = type(entry)
                if t not in value_types:
                    value_types.append(t)
            if dict in value_types or list in value_types:
                result = "Tuple("
                unique_specs = []
                for entry in item:
                    spec = self._get_json_value_spec(entry, depth)
                    if not spec.startswith("Array") and not spec.startswith("Tuple") and not spec.startswith("JSON"):
                        result += f"Nullable({spec}), "
                    else:
                        result += f"{spec}, "
                    if spec not in unique_specs:
                        unique_specs.append(spec)

                # Return an array if all specs are the same
                if len(unique_specs) == 1:
                    return f"Array({self._get_json_value_spec(item[0], depth=depth)})"
                result = result[:-2] + ")"
                return result
            else:
                if str in value_types:
                    return "Array(Nullable(String))"
                elif float in value_types:
                    if bool not in value_types:
                        return "Array(Nullable(Float64))"
                    else:
                        return "Array(Nullable(String))"
                elif int in value_types:
                    return "Array(Nullable(Int64))"
                elif bool in value_types:
                    return "Array(Nullable(Bool))"
                else:
                    return "Array(Nullable(String))"
        elif item is None:
            return "String"

    def _get_row_posititons(self, col, row_count):
        """
        Returns bytes corresponding to the position of specs between records.
        """
        result = [255] * row_count
        count = 0
        skip = len(col) - len([v for v in col.keys()
                               if v.startswith("String") or v.startswith("Tuple")])
        for spec in col:
            if count == skip:
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

    def _unfold_json_item(self, obj, depth, result={}, row_count=0):
        """
        Converts a single record into an intermeditary format stored in result.
        """
        for k in obj:
            if obj[k] is not None:
                obj_res = self._normalize_json(obj[k])
                for obj_k in obj_res:
                    if f"{k}.{obj_k}" not in result:
                        result[f"{k}.{obj_k}"] = {}
                    spec = self._get_json_value_spec(obj_res[obj_k], depth)
                    if spec not in result[f"{k}.{obj_k}"]:
                        result[f"{k}.{obj_k}"][spec] = {
                            "values": [], "positions": []}
                    result[f"{k}.{obj_k}"][spec]["values"].append(
                        obj_res[obj_k])
                    result[f"{k}.{obj_k}"][spec]["positions"].append(row_count)
        return result

    def _unfold_json(self, items, depth):
        """
        Converts the passed dictionary into an intermediary format.
        """
        result = {}
        for row, obj in enumerate(items):
            result = self._unfold_json_item(obj, depth, result, row)
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

    def _preprocess_array(self, values, array_type):
        """
        Preprocesses array values for insert.
        """
        insert = []
        if array_type.startswith("Array"):
            for item in values:
                insert.append(self._preprocess_array(item, array_type[6:-1]))
            return insert
        
        if "String" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if isinstance(elem, str):
                        arr.append(elem)
                    elif isinstance(elem, bool):
                        arr.append(str(elem).lower())
                    elif elem is None:
                        arr.append(None)
                    else:
                        arr.append(str(elem))
                insert.append(arr)
        elif "Int64" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if elem is None:
                        arr.append(0)
                    else:
                        arr.append(int(elem))
                insert.append(arr)
        elif "Float64" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if elem is None:
                        arr.append(0)
                    else:
                        arr.append(float(elem))
                insert.append(arr)
        elif "Bool" in array_type:
            for item in values:
                arr = []
                for elem in item:
                    if elem is not None:
                        arr.append(bool(elem))
                insert.append(arr)
        else:
            insert = values

        return insert


def create_newjson_column(spec, column_by_spec_getter, column_options):
    return NewJsonColumn(column_by_spec_getter, **column_options)
