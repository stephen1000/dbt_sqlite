#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "dbt-sqlite"
package_version = "0.0.1"
description = """The sqlite adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Stephen Lowery",
    author_email="slowery@ippon.fr",
    url="https://github.com/stephen1000/dbt_sqlite/",
    packages=find_packages(),
    package_data={
        "dbt": ["include/sqlite/macros/*.sql", "include/sqlite/dbt_project.yml",]
    },
    install_requires=["dbt-core==0.16.1rc1"],
    py
)
