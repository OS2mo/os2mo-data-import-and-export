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
        "os2mo_helpers",
        "fixture_generator",
    ],
    package_data={
        "fixture_generator": ["navne/*.txt", "*.p"],
    },
    zip_safe=False,
    install_requires=[
        "certifi",
        "chardet",
        "click",
        "idna",
        "requests",
        "urllib3",
        "anytree",
        "freezegun",
    ]
)
