import auxon_sdk as auxon
import os
from dataclasses import dataclass


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

def test_basic_ingest():
    cfg = auxon.IngestPluginConfig(SimpleConfig, "TEST_")
    print(cfg.plugin)

    c = cfg.connect_and_authenticate()

    assert cfg.plugin.int_val == None
    assert cfg.plugin.str_val == None

    tl = auxon.TimelineId()
    c.switch_timeline(tl)
    c.send_timeline_attrs("test", {"a": 1, "b": "yay"})
    c.send_event("ev1", 1, {"q": 1, "r": "whee"})
    c.send_event("ev2", 2, {"q": 1, "r": "yo"})

def test_env_var_config():
    cfg = auxon.IngestPluginConfig(SimpleConfig, "TEST_")

    assert cfg.plugin.str_val == None
    assert cfg.plugin.int_val == None
    assert cfg.plugin.float_val == None
    assert cfg.plugin.bool_val == None
    assert cfg.plugin.base_str_val == None
    assert cfg.plugin.base_int_val == None
    assert cfg.plugin.base_float_val == None
    assert cfg.plugin.base_bool_val == None

    os.environ["TEST_STR_VAL"] = "str"
    os.environ["TEST_INT_VAL"] = "42"
    os.environ["TEST_FLOAT_VAL"] = "3.14"
    os.environ["TEST_BOOL_VAL"] = "true"
    cfg = auxon.IngestPluginConfig(SimpleConfig, "TEST_")

    assert cfg.plugin.str_val == "str"
    assert cfg.plugin.int_val == 42
    assert cfg.plugin.float_val == 3.14
    assert cfg.plugin.bool_val == True
    assert cfg.plugin.base_str_val == None
    assert cfg.plugin.base_int_val == None
    assert cfg.plugin.base_float_val == None
    assert cfg.plugin.base_bool_val == None

    os.environ["TEST_BASE_STR_VAL"] = "str"
    os.environ["TEST_BASE_INT_VAL"] = "42"
    os.environ["TEST_BASE_FLOAT_VAL"] = "3.14"
    os.environ["TEST_BASE_BOOL_VAL"] = "true"
    cfg = auxon.IngestPluginConfig(SimpleConfig, "TEST_")

    assert cfg.plugin.str_val == "str"
    assert cfg.plugin.int_val == 42
    assert cfg.plugin.float_val == 3.14
    assert cfg.plugin.bool_val == True
    assert cfg.plugin.base_str_val == "str"
    assert cfg.plugin.base_int_val == 42
    assert cfg.plugin.base_float_val == 3.14
    assert cfg.plugin.base_bool_val == True
