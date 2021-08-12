# -- coding: utf-8 --

from setuptools import setup

setup(
    name="os2mo_data_import",
    version="0.0.1",
    description="A set of tools for OS2MO data import and export",
    author="Magenta ApS",
    author_email="info@magenta.dk",
    license="MPL 2.0",
    entry_points={},
    packages=[
        "os2mo_data_import",
        "integration_abstraction",
        "os2mo_helpers",
        "mox_helpers",
        "kle",
    ],
    package_data={},
    zip_safe=False,
    install_requires=[
        "aiohttp",
        "anytree",
        "certifi",
        "chardet",
        "click",
        "freezegun",
        "idna",
        "openpyxl",
        "requests",
        "xlsxwriter",
        "xmltodict",
        "more_itertools",
    ]
)
