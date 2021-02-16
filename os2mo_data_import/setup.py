# -- coding: utf-8 --

from setuptools import setup

setup(
    name="os2mo_data_import",
    version="0.0.1",
    description="A set of tools for OS2MO data import and export",
    author="Magenta ApS",
    author_email="info@magenta.dk",
    license="MPL 2.0",
    entry_points={
        'console_scripts': [
            'mo-populate = fixture_generator.populate_mo:main',
        ],
    },
    packages=[
        "os2mo_data_import",
        "integration_abstraction",
        "os2mo_helpers",
        "mox_helpers",
        "fixture_generator",
        "kle",
    ],
    package_data={
        "fixture_generator": ["navne/*.txt", "*.p"],
    },
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
