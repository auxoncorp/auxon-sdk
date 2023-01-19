from uuid import UUID
def format_json_attr_val(val):
    r"""Produce a human-readable representation of an attribute value

    :param val: Python value representation directly interpreted from AttrVal's JSON form
    """
    if isinstance(val, str):
        return val

    try:
        if 'TimelineId' in val:
            return "{}".format(val['TimelineId'])
        elif 'BigInt' in val:
            return "{}".format(val['BigInt'])
        elif 'Timestamp' in val:
            return "{}".format(val['Timestamp'])
        elif 'EventCoordinate' in val:
            ec = val['EventCoordinate']
            out_str = "{}:".format(UUID(ec['timeline_id']).hex)
            id_bytes = ec['id']
            # Skip the leading zeros
            cursor = len(id_bytes)
            for i, b in enumerate(id_bytes):
                if b != 0:
                    cursor = i
                    break
            if cursor == len(id_bytes):
                out_str += "0"
            else:
                for b in id_bytes[cursor:]:
                    out_str += "{:02x}".format(b)
            return out_str
        else:
            return "{}".format(val)
    except TypeError:
        return "{}".format(val)
