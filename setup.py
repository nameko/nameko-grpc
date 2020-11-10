#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from codecs import open

from setuptools import find_packages, setup


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.md"), "r", "utf-8") as handle:
    readme = handle.read()


setup(
    name="nameko-grpc",
    version="1.1.0",
    description="Nameko gRPC extensions",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Matt Yule-Bennett",
    url="http://github.com/nameko/nameko-grpc",
    packages=find_packages(exclude=["test"]),
    install_requires=["nameko>=3.0.0-rc9", "h2>=3", "grpcio", "protobuf"],
    extras_require={"dev": ["pytest", "grpcio-tools", "wrapt", "zmq"]},
    zip_safe=True,
    license="Apache License, Version 2.0",
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ],
)
