from urllib.parse import urlparse, urlencode
from copy import deepcopy

class Modality:
    modality_url = "http://localhost:14181/v1"
    auth_token = None

    def __init__(self, modality_url=None, auth_token=None):
        # TODO read from cli config file if present
        if modality_url:
            self.modality_url = modality_url
        self.auth_token = auth_token

    def events_data_frame(self, **kwargs):
        import pandas as pd
        url = self._events_data_frame_url(**kwargs)
        dtype_dict = {'event.timestamp': 'datetime64[ns]',
                      'segment.latest_receive_time': 'datetime64[ns]'}
        df = pd.read_json(url,
                          lines=True,
                          dtype=dtype_dict,
                          storage_options={"X-Auxon-Auth-Token": self.auth_token})
        return df

    def _events_data_frame_url(self, **kwargs):
        # "http://127.0.0.1:14181/v1/inspection/events_data_frame?split_by_segment=true&event_filter=_.name%3D%27Start%2A%27&include_timeline_attrs=true&timeline_filter=_.name%3D%27monitor%27%0D%0A"

        url_params = []
        if 'workspace' in kwargs:
            url_params.append(('workspace_name', kwargs['workspace']))

        if 'workspace_name' in kwargs:
            url_params.append(('workspace_name', kwargs['workspace_name']))

        if 'workspace_version_id' in kwargs:
            url_params.append(('workspace_version_id', kwargs['workspace_version_id']))

        if 'split_by_segment' in kwargs and kwargs['split_by_segment']:
            url_params.append(('split_by_segment', 'true'))

        if 'include_newest_segment' in kwargs and kwargs['include_newest_segment']:
            url_params.append(('include_newest_segment_in_workspace', 'true'))

        if 'segments' in kwargs:
            for seg in kwargs['segments']:
                url_params.append(('segments[]', seg))

        if 'include_timeline_attrs' in kwargs and kwargs['include_timeline_attrs']:
            url_params.append(('include_timeline_attrs', 'true'))

        if 'event_filter' in kwargs:
            url_params.append(('event_filter', kwargs['event_filter']))

        if 'timeline_filter' in kwargs:
            url_params.append(('timeline_filter', kwargs['timeline_filter']))

        return self._modality_url("inspection/events_data_frame", url_params)

    def _modality_url(self, endpoint, query_params):
        url = self.modality_url
        if not url.endswith("/"):
            url += "/"
        url += endpoint
        if query_params:
            url += "?" + urlencode(query_params)

        return url

