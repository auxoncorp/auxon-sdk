from urllib.parse import urlparse, urlencode
from copy import deepcopy
import appdirs
import os
import os.path
from urllib.request import urlopen, Request
import json


class Modality:
    modality_url = "http://localhost:14181/v1"
    auth_token = None
    default_attrs = {
        "TIMELINE": {
            "ID": "timeline.id",
            "NAME": "timeline.name",
            "DESCRIPTION": "timeline.description",
            "SOURCE_FILE": "timeline.source.file",
            "SOURCE_LINE": "timeline.source.line",
            "TIME_DOMAIN": "timeline.time_domain",
            "TIME_RESOLUTION": "timeline.time_resolution",
            "RUN_ID": "timeline.run_id",

            "INGEST_SOURCE": "timeline.ingest_source",
            "INGEST_EDGE_ID": "timeline.ingest_edge_id",
            "RECEIVE_TIME": "timeline.receive_time",
        },

        "EVENT": {
            "COORDINATES": "event.coordinates",
            "NAME": "event.name",
            "DESCRIPTION": "event.description",
            "MESSAGE": "event.message",
            "LOGICAL_TIME": "event.logical_time",
            "TIMESTAMP": "event.timestamp",
            "NONCE": "event.nonce",
            "IS_FAILURE": "event.is_failure",
            "IS_EXPECTATION": "event.is_expectation",
            "SEVERITY": "event.severity",
            "PAYLOAD": "event.payload",
            "SOURCE_FILE": "event.source.file",
            "SOURCE_LINE": "event.source.line",

            # The ID of a remote timeline that is known to causally precede this event. Must also come
            # with at least one of REMOTE_LOGICAL_TIME, REMOTE_TIMESTAMP, or REMOTE_NONCE.
            "REMOTE_TIMELINE_ID": "event.interaction.remote_timeline_id",
            "REMOTE_LOGICAL_TIME": "event.interaction.remote_logical_time",
            "REMOTE_TIMESTAMP": "event.interaction.remote_timestamp",
            "REMOTE_NONCE": "event.interaction.remote_nonce",

            # Expected to be the i128 representation of a mutation's UUID bytes (interpreted in a little-endian manner).
            "MUTATION_ID": "event.mutation.id",
            # Whether the event-name-associated stage of the mutation lifecycle happened correctly or not. Expected to be boolean.
            "MUTATION_SUCCESS": "event.mutation.success",
            # Expected to be the i128 representation of a mutator id's UUID bytes (interpreted in a little-endian manner).
            "MUTATOR_ID": "event.mutator.id",
        },

        "MUTATOR": {
            "ID": "mutator.id",
            "NAME": "mutator.name",
            "DESCRIPTION": "mutator.description",
            "LAYER": "mutator.layer",
            "GROUP": "mutator.group",
            "STATEFULNESS": "mutator.statefulness",
            "OPERATION": "mutator.operation",
            "SAFETY": "mutator.safety",
            "SOURCE_FILE": "mutator.source.file",
            "SOURCE_LINE": "mutator.source.line",

            "MUTATION_EDGE_ID": "mutator.mutation_edge_id",
        },

        "SEGMENT": {
            "NAME": "segment.name",
            "RULE_NAME": "segment.rule_name",
            "WORKSPACE_VERSION_ID": "segment.workspace_version_id",
            "LATEST_RECEIVE_TIME": "segment.latest_receive_time",
        },
    }
    default_attr_keys = list(default_attrs["TIMELINE"].values()) + \
                        list(default_attrs["EVENT"].values()) + \
                        list(default_attrs["MUTATOR"].values()) + \
                        list(default_attrs["SEGMENT"].values())

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
            cli_config_dir = appdirs.user_config_dir("modality_cli")
            token_file = os.path.join(cli_config_dir, ".user_auth_token")

            # Prefer env-var over user-global config file
            if "MODALITY_AUTH_TOKEN" in os.environ:
                self.auth_token = os.environ.get('MODALITY_AUTH_TOKEN').rstrip()
            elif os.path.exists(token_file):
                with open(token_file, 'r') as file:
                    self.auth_token = file.read().rstrip()

    def events_data_frame(self,
                          workspace_name=None, workspace_version_id=None, segments=None,
                          only_newest_segment_in_workspace=None, timeline_filter=None,
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
                                          segments=segments,
                                          only_newest_segment_in_workspace=only_newest_segment_in_workspace,
                                          timeline_filter=timeline_filter,
                                          split_by_segment=split_by_segment, event_filter=event_filter,
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
                                             workspace_name=None, workspace_version_id=None, segments=None,
                                             only_newest_segment_in_workspace=None, timeline_filter=None,
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

        url = self._event_value_distributions_data_frame_url(workspace_name=workspace_name,
                                                             workspace_version_id=workspace_version_id,
                                                             segments=segments,
                                                             only_newest_segment_in_workspace=only_newest_segment_in_workspace,
                                                             timeline_filter=timeline_filter,
                                                             group_keys=group_keys, event_filter=event_filter,
                                                             include_attrs=include_attrs)

        import pandas as pd
        dtype_dict = {'event.timestamp': 'datetime64[ns]',
                      'segment.latest_receive_time': 'datetime64[ns]'}

        storage_options = {}
        if self.auth_token:
            storage_options['X-Auxon-Auth-Token'] = self.auth_token

        df = pd.read_json(url, lines=True, dtype=dtype_dict, storage_options=storage_options)
        return df

    def experiment_overview(self, experiment_name,
                            workspace_name=None, workspace_version_id=None, segments=None,
                            only_newest_segment_in_workspace=None, timeline_filter=None,
                            ):
        r"""

        :param str experiment_name: The star of the show
        :param str workspace_name: Which workspace's data are we focusing on to find the effects of this experiment?
        :param str workspace_version_id: Which workspace's data are we focusing on to find the effects of this experiment?
        :param array[object] segments: A collection of WorkspaceSegment objects to narrow the focus of the search for effects of the experiment.
        :param bool only_newest_segment_in_workspace: If you specified the workspace, limit to the newest segment in that workspace.
        :param timeline_filter: Limit to events logged on timelines which match this Modality filter expression. e.g. "_.name = 'bar'".
        :return:
        """
        u = self._modality_url("experiment/get_experiment", [])
        custom_headers = {
            "Content-Type": "application/json"
        }
        if self.auth_token:
            custom_headers["X-Auxon-Auth-Token"] = self.auth_token

        req_body = {'name': experiment_name, 'scope': {}}
        if segments:
            req_body['scope']['WorkspaceSegments'] = {"timeline_filter": timeline_filter, "segments": segments}
        elif only_newest_segment_in_workspace:
            if workspace_version_id:
                req_body['scope']['NewestSegmentInWorkspace'] = {"timeline_filter": timeline_filter,
                                                  "workspace": {"Version": workspace_version_id}}
            elif workspace_name:
                req_body['scope']['NewestSegmentInWorkspace'] = {"timeline_filter": timeline_filter, "workspace": {"Name": workspace_name}}
            else:
                raise ValueError('Either workspace_version_id or workspace_name is required if only_newest_segment_in_workspace is True')
        elif workspace_version_id:
            req_body['scope']['Workspace'] = {"timeline_filter": timeline_filter,
                                              "workspace": {"Version": workspace_version_id}}
        elif workspace_name:
            req_body['scope']['Workspace'] = {"timeline_filter": timeline_filter, "workspace": {"Name": workspace_name}}
        else:
            req_body['scope']['Global'] = {"timeline_filter": timeline_filter}
        req = Request(
            u,
            json.dumps(req_body).encode('ascii'),
            custom_headers,
            method='POST'
        )
        with urlopen(req) as response:
            json_response = json.load(response)
            return json_response

        return None

    def workspace_segments(self, workspace_name=None, workspace_version_id=None):
        r"""Retrieve the workspace segments for the given workspace"""
        # Make sure we have the workspace version id
        workspace_version_id = self._resolve_workspace_version_id(workspace_name=workspace_name, workspace_version_id=workspace_version_id)
        custom_headers = {
            "Content-Type": "application/json"
        }
        if self.auth_token:
            custom_headers["X-Auxon-Auth-Token"] = self.auth_token

        list_segs_u = self._modality_url("inspection/list_workspace_segments", [])
        req_body = {"workspace_version_id": workspace_version_id}
        req = Request(
            list_segs_u,
            json.dumps(req_body).encode('ascii'),
            custom_headers,
            method='POST'
        )
        with urlopen(req) as response:
            json_resp = json.load(response)
            if 'Ok' in json_resp:
                if 'segments' in json_resp['Ok']:
                    return json_resp['Ok']['segments']
                else:
                    return []
            else:
                raise Exception("Unsuccessful attempt at getting workspace segments. {}".format(json_resp['Err']))

    def _resolve_workspace_version_id(self, workspace_name=None, workspace_version_id=None):
        if workspace_version_id:
            return workspace_version_id

        custom_headers = {
            "Content-Type": "application/json"
        }
        if self.auth_token:
            custom_headers["X-Auxon-Auth-Token"] = self.auth_token

        if not workspace_name:
            raise ValueError("Either workspace_version_id or workspace_name must be provided")

        get_ws_u = self._modality_url("workspace/get_workspace_definition", [])
        req_body = {"workspace_name": workspace_name}
        req = Request(
            get_ws_u,
            json.dumps(req_body).encode('ascii'),
            custom_headers,
            method='POST'
        )
        with urlopen(req) as response:
            json_resp = json.load(response)
            if 'Ok' in json_resp:
                return json_resp['Ok']['version']
            else:
                raise Exception("Unsuccessful attempt at getting workspace. {}".format(json_resp['Err']))

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
            url_params.append(
                ('only_newest_segment_in_workspace', 'true' if only_newest_segment_in_workspace else 'false'))

        if timeline_filter:
            url_params.append(('timeline_filter', timeline_filter))

        return url_params

    def _events_data_frame_url(self,
                               workspace_name=None, workspace_version_id=None, segments=None,
                               only_newest_segment_in_workspace=None, timeline_filter=None,
                               split_by_segment=None, event_filter=None, include_timeline_attrs=None,
                               include_attrs=None):

        url_params = self._flat_scope_url_params(workspace_name=workspace_name,
                                                 workspace_version_id=workspace_version_id, segments=segments,
                                                 only_newest_segment_in_workspace=only_newest_segment_in_workspace,
                                                 timeline_filter=timeline_filter)

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
            workspace_name=None, workspace_version_id=None, segments=None, only_newest_segment_in_workspace=None,
            timeline_filter=None,
            group_keys=None, event_filter=None, include_attrs=None):

        url_params = self._flat_scope_url_params(workspace_name=workspace_name,
                                                 workspace_version_id=workspace_version_id, segments=segments,
                                                 only_newest_segment_in_workspace=only_newest_segment_in_workspace,
                                                 timeline_filter=timeline_filter)

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
