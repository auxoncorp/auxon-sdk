import os
from setuptools import Extension, setup
from Cython.Build import cythonize

modality_extension = Extension(
    name='modality.sdk',
    sources=[f'{os.environ.get("MODALITY_SDK_CYTHON_OUT_DIR")}/modality.pyx'],
    libraries=['modality'],
    library_dirs=['../target/release', '../target/debug', '/usr/lib'],
    include_dirs=['.', f'{os.environ.get("MODALITY_SDK_CAPI_OUT_DIR")}/include', '/usr/include'],
)

setup(
    name='modality_sdk',
    ext_modules=cythonize([modality_extension], language_level = '2')
)
