# -- coding: utf-8 --

from setuptools import setup

setup(
    name="os2mo_helpers",
    version="0.0.1",
    description="Helper for interfacing os2mo",
    author="Magenta ApS",
    author_email="info@magenta.dk",
    license="MPL 2.0",
    packages=["os2mo_helpers"],
    zip_safe=False,
    install_requires=[
        "anytree >= 2.4.3"
    ]
)
