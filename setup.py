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
    'url': 'https://github.com/albertodonato/query-exporter',
    'packages': find_packages(),
    'include_package_data': True,
    'entry_points': {'console_scripts': [
        'query-exporter = query_exporter.main:script']},
    'test_suite': 'query-exporter',
    'install_requires': [
        'aiohttp',
        'aiopg',
        'prometheus-client',
        'prometheus-aioexporter',
        'PyYaml',
        'toolrack'],
    'tests_require': ['asynctest'],
    'keywords': 'sql metric prometheus exporter',
    'classifiers': [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.5',
        'Topic :: Utilities']}

setup(**config)
