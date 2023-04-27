# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    name="Apache Beam Example Taxi Project",
    version="1.0",
    description="A Python Apache Beam pipeline.",
    author="David James",
    author_email="david.james@informatik.hs-fulda,de",
    packages=find_packages(),
    install_requires=requirements,
)
