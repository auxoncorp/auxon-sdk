import modality.client

def test_events_data_frame_url_test():
    m = modality.client.Modality()

    u = m._events_data_frame_url()
    assert u == "http://localhost:14181/v1/inspection/events_data_frame"

    u = m._events_data_frame_url(workspace_name = 'default')
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?workspace_name=default"

    u = m._events_data_frame_url(workspace_version_id= '12345')
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?workspace_version_id=12345"

    u = m._events_data_frame_url(segments = ["a", "b"])
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?segments=a&segments=b"

    u = m._events_data_frame_url(only_newest_segment_in_workspace = True)
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?only_newest_segment_in_workspace=true"

    u = m._events_data_frame_url(timeline_filter = "_.name='foo'")
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?timeline_filter=_.name%3D%27foo%27"

    u = m._events_data_frame_url(split_by_segment = True)
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?split_by_segment=true"

    u = m._events_data_frame_url(event_filter = "_.name='foo'")
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?event_filter=_.name%3D%27foo%27"

    u = m._events_data_frame_url(include_timeline_attrs = True)
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?include_timeline_attrs=true"

    u = m._events_data_frame_url(include_attrs = ["event.a", "event.b"])
    assert u == "http://localhost:14181/v1/inspection/events_data_frame?include_attrs=event.a&include_attrs=event.b"

def test_event_value_distributions_data_frame_url_test():
    m = modality.client.Modality()

    u = m._event_value_distributions_data_frame_url()
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame"

    u = m._event_value_distributions_data_frame_url(workspace_name = 'default')
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?workspace_name=default"

    u = m._event_value_distributions_data_frame_url(workspace_version_id= '12345')
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?workspace_version_id=12345"

    u = m._event_value_distributions_data_frame_url(segments = ["a", "b"])
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?segments=a&segments=b"

    u = m._event_value_distributions_data_frame_url(only_newest_segment_in_workspace = True)
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?only_newest_segment_in_workspace=true"

    u = m._event_value_distributions_data_frame_url(timeline_filter = "_.name='foo'")
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?timeline_filter=_.name%3D%27foo%27"

    u = m._event_value_distributions_data_frame_url(group_keys = ["segment_name", "timeline_name", "event_name"])
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?group_keys=segment_name&group_keys=timeline_name&group_keys=event_name"
    u = m._event_value_distributions_data_frame_url(event_filter = "_.name='foo'")
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?event_filter=_.name%3D%27foo%27"

    u = m._event_value_distributions_data_frame_url(include_attrs = ["event.a", "event.b"])
    assert u == "http://localhost:14181/v1/inspection/event_value_distributions_data_frame?include_attrs=event.a&include_attrs=event.b"
