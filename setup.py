# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()
test_requirements = []
setup(
    author = "Francois Vanderseypen",
    author_email = 'swa@orbifold.net',
    python_requires = '>=3.8',
    name = 'entity_resolution',
    version = '1.3.0',
    description = 'Graph Entity Resolution Kit',
    long_description = readme,
    url = 'https://github.com/Orbifold/entity-resolution.git',
    license = license,
    packages = find_packages(exclude = ('entity_resolution', 'entity_resolution.*')),
    test_suite = 'tests',
    tests_require = test_requirements,
)
