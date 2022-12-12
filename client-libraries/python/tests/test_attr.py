from modality.attr import format_json_attr_val
def test_format_json_attr_val():

    f = format_json_attr_val({"TimelineId": "3cd8734a-1747-4cfd-a305-2e8b3aa41c8f"})
    assert f == "3cd8734a-1747-4cfd-a305-2e8b3aa41c8f"

    f = format_json_attr_val("abc")
    assert f == "abc"

    f = format_json_attr_val(123)
    assert f == "123"

    f = format_json_attr_val({"BigInt": "170141183460469231731687303715884105727"})
    assert f == "170141183460469231731687303715884105727"

    f = format_json_attr_val(False)
    assert f == "False"
    f = format_json_attr_val(True)
    assert f == "True"

    f = format_json_attr_val({"Timestamp": "456"})
    assert f == "456"

    f = format_json_attr_val(3.12)
    assert f == "3.12"

    f = format_json_attr_val({"EventCoordinate": {"timeline_id": "91e77012-d22b-4653-aaf7-eba525711637", "id": [0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]}})
    assert f == "91e77012d22b4653aaf7eba525711637:0102030405060708"

    f = format_json_attr_val({"EventCoordinate": {"timeline_id": "91e77012-d22b-4653-aaf7-eba525711637", "id": [255, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]}})
    assert f == "91e77012d22b4653aaf7eba525711637:ff000000000000000102030405060708"

    f = format_json_attr_val({"EventCoordinate": {"timeline_id": "91e77012-d22b-4653-aaf7-eba525711637", "id": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}})
    assert f == "91e77012d22b4653aaf7eba525711637:0"

