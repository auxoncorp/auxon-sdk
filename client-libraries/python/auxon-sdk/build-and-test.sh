set -ex

pip install maturin pytest
maturin build -i python3.8
pip uninstall auxon_sdk -y

pip install target/wheels/auxon_sdk-*.whl
pytest
