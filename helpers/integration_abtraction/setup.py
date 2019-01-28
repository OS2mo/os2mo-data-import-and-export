# -- coding: utf-8 --

from setuptools import setup

setup(
    name="integration_abstraction",
    version="0.0.1",
    description="Helper for interfacing os2mo",
    author="Magenta ApS",
    author_email="info@magenta.dk",
    license="MPL 2.0",
    packages=["integration_abstraction"],
    zip_safe=False,
    install_requires=[
        "requests"
    ]
)
