# Python Client Library for Auxon

This is a library for the python programming language used to interact with
Auxon's suite of products.

It currently provides read access to data stored within the Modality database,
both as raw trace data and in-place statistical analyses.

The primary interface is in `modality.client`. The functions
`events_dataframe`, `event_value_distributions_dataframe`,
`experiment_overview`, and `workspace_segments` allow access to trace data
stored within modality. See the inline documentation for more information, eg.:

```python
from modality import client
help(client)
```

## Running tests

```python
pip3 install -U pytest
pip3 install -U appdirs
pytest
```
