from urllib.parse import urlparse, urlencode
from copy import deepcopy
import appdirs
import os.path

class Modality:
    modality_url = "http://localhost:14181/v1"
    auth_token = None

    def __init__(self, modality_url=None, auth_token=None):
        # TODO read from cli config file if present
        if modality_url:
            self.modality_url = modality_url
        self.auth_token = auth_token

        if not self.modality_url:
            modality_toml = appdirs.user_config_dir("modality.toml")
            if os.path.exists(modality_toml):
                modality_toml_dict = toml.load(modality_toml)
                if modality_toml_dict['modalityd']:
                    self.modality_url = modality_toml_dict['modalityd']

        if not self.auth_token:
            # TODO: get token from the env too
            cli_config_dir = appdirs.user_config_dir("modality_cli")
            token_file = os.path.join(cli_config_dir, ".user_auth_token")
            if os.path.exists(token_file):
                with open(token_file, 'r') as file:
                    self.auth_token = file.read().rstrip()


    def events_data_frame(self,
                          workspace_name=None, workspace_version_id=None, segments=None, only_newest_segment_in_workspace=None, timeline_filter=None,
                          split_by_segment=None, event_filter=None, include_timeline_attrs=None, include_attrs=None):
        r"""Load events from Modality into a pandas dataframe.

        :param str workspace_name: Limit fetched events to those contained in this workspace.
        :param str workspace_version_id: Limit fetched events to those contained in this workspace.
        :param array[str] segments: Limit to events from these segments. Workspace must also be specified.
        :param bool only_newest_segment_in_workspace: If you specified the workspace, limit to the newest segment in that workspace.
        :param timeline_filter: Limit to events logged on timelines which match this Modality filter expression. e.g. "_.name = 'bar'".
        :param bool split_by_segment: Split the results into segments, for all available segments. Include columns with segment information in the dataframe.
        :param event_filter: Limit to events passing this Modality filter expression. e.g. "_.name = 'foo'".
        :param bool include_timeline_attrs: Include "timeline.*" columns in the dataframe.
        :param array[str] include_attrs: Include these specific attrs on each event.
        """

        url = self._events_data_frame_url(workspace_name=workspace_name, workspace_version_id=workspace_version_id,
                                          segments=segments,only_newest_segment_in_workspace=only_newest_segment_in_workspace, timeline_filter=timeline_filter,
                                          split_by_segment=split_by_segment, event_filter = event_filter,
                                          include_timeline_attrs=include_timeline_attrs, include_attrs=include_attrs)

        import pandas as pd
        dtype_dict = {'event.timestamp': 'datetime64[ns]',
                      'segment.latest_receive_time': 'datetime64[ns]'}

        storage_options = {}
        if self.auth_token:
            storage_options['X-Auxon-Auth-Token'] = self.auth_token

        df = pd.read_json(url, lines=True, dtype=dtype_dict, storage_options=storage_options)
        return df

    def event_value_distributions_data_frame(self,
                                             workspace_name=None, workspace_version_id=None, segments=None, only_newest_segment_in_workspace=None, timeline_filter=None,
                                             group_keys=None, event_filter=None, include_attrs=None):
        r"""Load statistical sketch of attribute values from Modality into a pandas dataframe.

        :param str workspace_name: Limit fetched events to those contained in this workspace.
        :param str workspace_version_id: Limit fetched events to those contained in this workspace.
        :param array[str] segments: Limit to events from these segments. Workspace must also be specified.
        :param bool only_newest_segment_in_workspace: If you specified the workspace, limit to the newest segment in that workspace.
        :param timeline_filter: Limit to events logged on timelines which match this Modality filter expression. e.g. "_.name = 'bar'".
        :param array[str] group_keys: Group the events into buckets by these criteria. Allowed values: "segment_name", "timeline_name", "event_name".
                                      Always groups by attr key as well. Order doesn't matter.
        :param event_filter: Limit to events passing this Modality filter expression. e.g. "_.name = 'foo'".
        :param array[str] include_attrs: Include these specific attrs on each event.
        """

        url = self._event_value_distributions_data_frame_url(workspace_name=workspace_name, workspace_version_id=workspace_version_id,
                                                             segments=segments,only_newest_segment_in_workspace=only_newest_segment_in_workspace,
                                                             timeline_filter=timeline_filter,
                                                             group_keys=group_keys, event_filter=event_filter, include_attrs=include_attrs)

        import pandas as pd
        dtype_dict = {'event.timestamp': 'datetime64[ns]',
                      'segment.latest_receive_time': 'datetime64[ns]'}

        storage_options = {}
        if self.auth_token:
            storage_options['X-Auxon-Auth-Token'] = self.auth_token

        df = pd.read_json(url, lines=True, dtype=dtype_dict, storage_options=storage_options)
        return df

    def _flat_scope_url_params(self, workspace_name=None, workspace_version_id=None, segments=None,
                               only_newest_segment_in_workspace=None, timeline_filter=None):
        url_params = []

        # scope
        if workspace_name:
            url_params.append(('workspace_name', workspace_name))

        if workspace_version_id:
            url_params.append(('workspace_version_id', workspace_version_id))

        if segments:
            for seg in segments:
                url_params.append(('segments', seg))

        if only_newest_segment_in_workspace:
            url_params.append(('only_newest_segment_in_workspace', 'true' if only_newest_segment_in_workspace else 'false'))

        if timeline_filter:
            url_params.append(('timeline_filter', timeline_filter))

        return url_params

    def _events_data_frame_url(self,
                               workspace_name=None, workspace_version_id=None, segments=None, only_newest_segment_in_workspace=None, timeline_filter=None,
                               split_by_segment=None, event_filter=None, include_timeline_attrs=None, include_attrs=None):

        url_params = self._flat_scope_url_params(workspace_name=workspace_name, workspace_version_id=workspace_version_id, segments=segments,
                                                 only_newest_segment_in_workspace=only_newest_segment_in_workspace, timeline_filter=timeline_filter)

        if split_by_segment:
            url_params.append(('split_by_segment', 'true' if split_by_segment else 'false'))

        if event_filter:
            url_params.append(('event_filter', event_filter))

        if include_timeline_attrs:
            url_params.append(('include_timeline_attrs', 'true' if include_timeline_attrs else 'false'))

        if include_attrs:
            for attr in include_attrs:
                url_params.append(('include_attrs', attr))

        return self._modality_url("inspection/events_data_frame", url_params)

    def _event_value_distributions_data_frame_url(
            self,
            workspace_name=None, workspace_version_id=None, segments=None, only_newest_segment_in_workspace=None, timeline_filter=None,
            group_keys=None, event_filter=None, include_attrs=None):

        url_params = self._flat_scope_url_params(workspace_name=workspace_name, workspace_version_id=workspace_version_id, segments=segments,
                                                 only_newest_segment_in_workspace=only_newest_segment_in_workspace, timeline_filter=timeline_filter)

        if group_keys:
            for group_key in group_keys:
                url_params.append(('group_keys', group_key))

        if event_filter:
            url_params.append(('event_filter', event_filter))

        if include_attrs:
            for attr in include_attrs:
                url_params.append(('include_attrs', attr))

        return self._modality_url("inspection/event_value_distributions_data_frame", url_params)

    def _modality_url(self, endpoint, query_params):
        url = self.modality_url
        if not url.endswith("/"):
            url += "/"
        url += endpoint
        if query_params:
            url += "?" + urlencode(query_params)

        return url

