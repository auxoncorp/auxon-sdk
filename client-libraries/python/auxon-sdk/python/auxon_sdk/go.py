import modality.client
m = modality.client.Modality()

df = m.events_data_frame()
print(df)

df = m.event_value_distributions_data_frame()
print(df)

df = m.event_value_distributions_data_frame(group_keys=["segment_name", "timeline_name", "event_name"], include_attrs=["event.sample"])
print(df)
