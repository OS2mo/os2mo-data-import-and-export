#
# Copyright (c) 2017, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sd-connector",
    version="0.0.1",
    author="Magenta ApS",
    author_email="info@magenta.dk",
    description="Connector library for SDLon webservices",
    license="MPL 2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.magenta.dk/rammearkitektur/os2mo-data-import-and-export",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'aiohttp',
        'xmltodict',
    ],
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
)
