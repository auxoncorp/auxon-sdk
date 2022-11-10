import modality.client

def test_events_data_frame_url_test():
    m = modality.client.Modality()

    u = m._events_data_frame_url()
    assert u == "http://localhost:14181/v1/inspection/events_data_frame"

    u = m._events_data_frame_url(workspace = 'default')
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?workspace_name=default"

    u = m._events_data_frame_url(workspace_name = 'default')
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?workspace_name=default"

    u = m._events_data_frame_url(split_by_segment = True)
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?split_by_segment=true"

    u = m._events_data_frame_url(include_newest_segment = True)
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?include_newest_segment_in_workspace=true"

    u = m._events_data_frame_url(segments = ["a", "b"])
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?segments%5B%5D=a&segments%5B%5D=b"

    u = m._events_data_frame_url(include_timeline_attrs = True)
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?include_timeline_attrs=true"

    u = m._events_data_frame_url(event_filter = "_.name='foo'")
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?event_filter=_.name%3D%27foo%27"
