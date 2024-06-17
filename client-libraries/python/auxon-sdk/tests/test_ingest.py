import auxon_sdk as auxon
from dataclasses import dataclass
import os
import tempfile
import unittest

@dataclass
class BaseConfig:
    base_str_val: str = None
    base_int_val: int = None
    base_float_val: float = None
    base_bool_val: bool = None

@dataclass
class SimpleConfig(BaseConfig):
    str_val: str = None
    int_val: int = None
    float_val: float = None
    bool_val: bool = None

@unittest.skip("This is exercised by the integration tests")
def test_basic_ingest():
    cfg = auxon.PluginConfig(SimpleConfig, "TEST_")
    c = cfg.connect_and_authenticate_ingest()

    assert cfg.plugin.int_val == None
    assert cfg.plugin.str_val == None

    tl = auxon.TimelineId.allocate()
    c.switch_timeline(tl)
    c.send_timeline_attrs("test", {"a": 1, "b": "yay"})
    c.send_event("ev1", 1, {"q": 1, "r": "whee"})
    c.send_event("ev2", 2, {"q": 1, "r": "yo"})

    c.flush()
    status = c.status()
    assert status.current_timeline == tl
    assert status.events_received == 2

def test_env_var_config():
    cfg = auxon.PluginConfig(SimpleConfig, "TEST_")
    assert cfg.plugin == SimpleConfig(
        str_val = None,
        int_val = None,
        float_val = None,
        bool_val = None,
        base_str_val = None,
        base_int_val = None,
        base_float_val = None,
        base_bool_val = None,
    )

    os.environ["TEST_STR_VAL"] = "str"
    os.environ["TEST_INT_VAL"] = "42"
    os.environ["TEST_FLOAT_VAL"] = "3.14"
    os.environ["TEST_BOOL_VAL"] = "true"

    cfg = auxon.PluginConfig(SimpleConfig, "TEST_")
    assert cfg.plugin == SimpleConfig(
        str_val = "str",
        int_val = 42,
        float_val = 3.14,
        bool_val = True,
        base_str_val = None,
        base_int_val = None,
        base_float_val = None,
        base_bool_val = None,
    )

    os.environ["TEST_BASE_STR_VAL"] = "bstr"
    os.environ["TEST_BASE_INT_VAL"] = "420"
    os.environ["TEST_BASE_FLOAT_VAL"] = "30.14"
    os.environ["TEST_BASE_BOOL_VAL"] = "false"

    cfg = auxon.PluginConfig(SimpleConfig, "TEST_")
    assert cfg.plugin == SimpleConfig(
        str_val = "str",
        int_val = 42,
        float_val = 3.14,
        bool_val = True,
        base_str_val = "bstr",
        base_int_val = 420,
        base_float_val = 30.14,
        base_bool_val = False,
    )


config_content = """
[ingest]
additional-timeline-attributes = ['a = 1']
override-timeline-attributes = ['c = true']
protocol-parent-url = 'modality-ingest-tls://auxon.io:9077'
allow-insecure-tls = true

[mutation]
additional-mutator-attributes = ['a = 1']
override-mutator-attributes = ['c = true']
protocol-parent-url = 'modality-mutation://auxon.io'
allow-insecure-tls = true

[metadata]
str-val = "str"
int-val = 42
float-val = 3.14
bool-val = true

base-str-val = "bstr"
base-int-val = 420
base-float-val = 30.14
base-bool-val = false
"""

def test_config_file():
    with tempfile.NamedTemporaryFile(mode = "w") as tmp:
        tmp.write(config_content)
        tmp.seek(0)
        
        os.environ["MODALITY_REFLECTOR_CONFIG"] = tmp.name

        cfg = auxon.PluginConfig(SimpleConfig, "TEST_")
        print(cfg.plugin)
        assert cfg.plugin == SimpleConfig(
            str_val = "str",
            int_val = 42,
            float_val = 3.14,
            bool_val = True,
            base_str_val = "bstr",
            base_int_val = 420,
            base_float_val = 30.14,
            base_bool_val = False,
        )


        # env vars override the config file
        os.environ["TEST_STR_VAL"] = "estr"
        os.environ["TEST_INT_VAL"] = "4200"
        os.environ["TEST_FLOAT_VAL"] = "300.14"
        os.environ["TEST_BOOL_VAL"] = "false"

        os.environ["TEST_BASE_STR_VAL"] = "ebstr"
        os.environ["TEST_BASE_INT_VAL"] = "42000"
        os.environ["TEST_BASE_FLOAT_VAL"] = "3000.14"
        os.environ["TEST_BASE_BOOL_VAL"] = "true"

        cfg = auxon.PluginConfig(SimpleConfig, "TEST_")
        assert cfg.plugin == SimpleConfig(
            str_val = "estr",
            int_val = 4200,
            float_val = 300.14,
            bool_val = False,
            base_str_val = "ebstr",
            base_int_val = 42000,
            base_float_val = 3000.14,
            base_bool_val = True,
        )
