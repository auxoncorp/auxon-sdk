set -ex

pip install maturin
maturin build -i python3.8
pip uninstall auxon_sdk -y
pip --verbose debug

pip install target/wheels/auxon_sdk-*.whl
pytest
