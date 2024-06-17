set -ex

pip install maturin
maturin build
pip uninstall auxon_sdk -y
pip install target/wheels/auxon_sdk-*.whl
pytest
