from setuptools import setup, find_packages

from query_exporter import __version__, __doc__ as description

config = {
    'name': 'query-exporter',
    'version': __version__,
    'license': 'GPLv3+',
    'description': description,
    'long_description': open('README.md').read(),
    'author': 'Alberto Donato',
    'author_email': 'alberto.donato@gmail.com',
    'maintainer': 'Alberto Donato',
    'maintainer_email': 'alberto.donato@gmail.com',
    'packages': find_packages(),
    'include_package_data': True,
    'entry_points': {'console_scripts': [
        'query-exporter = query_exporter.main:main']},
    'test_suite': 'lmetrics',
    'install_requires': [
        'aiohttp',
        'prometheus-client',
        'prometheus-aioexporter',
        'PyYaml',
        'toolrack'],
    'tests_require': ['asynctest'],
    'keywords': 'sql metric prometheus exporter'}

setup(**config)
