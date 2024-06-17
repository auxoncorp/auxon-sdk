python -m venv venv
source venv/bin/activate
pip install "maturin[patchelf] pytest"
maturin develop -E test
python -m pytest
