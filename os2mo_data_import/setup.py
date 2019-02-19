# -- coding: utf-8 --

from setuptools import setup

setup(
    name="os2mo_data_import",
    version="0.0.1",
    description="A set of tools for OS2MO data import and export",
    author="Magenta ApS",
    author_email="info@magenta.dk",
    license="MPL 2.0",
    packages=[
        "os2mo_data_import",
        "integration_abstraction",
        "os2mo_helpers"
    ],
    zip_safe=False,
    install_requires=[
        "certifi==2018.10.15",
        "chardet==3.0.4",
        "idna==2.7",
        "requests==2.21.0",
        "urllib3==1.24.1",
        "anytree >= 2.4.3"
    ]
)
