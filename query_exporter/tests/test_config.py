from operator import attrgetter

import yaml

from toolrack.testing import (
    TestCase,
    TempDirFixture)

from ..config import load_config, ConfigError


class LoadConfigTests(TestCase):

    def setUp(self):
        super().setUp()
        self.tempdir = self.useFixture(TempDirFixture())
        self.config = {
            'databases': {'db': {'dsn': 'dbname=foo'}},
            'metrics': {'m': {'type': 'gauge'}},
            'queries': {
                'q': {
                    'interval': 10,
                    'databases': ['db'],
                    'metrics': ['m'],
                    'sql': 'SELECT 1'}}}

    def test_load_databases_section(self):
        '''The 'databases' section is loaded from the config file.'''
        config = {
            'databases': {
                'db1': {'dsn': 'dbname=foo'},
                'db2': {'dsn': 'dbname=bar'}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config))
        with open(config_file) as fd:
            result = load_config(fd)
        database1, database2 = sorted(result.databases, key=attrgetter('name'))
        self.assertEqual(database1.name, 'db1')
        self.assertEqual(database1.dsn, 'dbname=foo')
        self.assertEqual(database2.name, 'db2')
        self.assertEqual(database2.dsn, 'dbname=bar')

    def test_load_databases_missing_dsn(self):
        '''An error is raised if the 'dsn' key is missing for a database.'''
        config = {'databases': {'db1': {}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(
            str(cm.exception), "Missing key 'dsn' for database 'db1'")

    def test_load_metrics_section(self):
        '''The 'metrics' section is loaded from the config file.'''
        config = {
            'metrics': {
                'metric1': {
                    'type': 'summary',
                    'description': 'metric one'},
                'metric2': {
                    'type': 'histogram',
                    'description': 'metric two',
                    'buckets': [10, 100, 1000]}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config))
        with open(config_file) as fd:
            result = load_config(fd)
        metric1, metric2 = sorted(result.metrics, key=attrgetter('name'))
        self.assertEqual(metric1.type, 'summary')
        self.assertEqual(metric1.description, 'metric one')
        self.assertEqual(metric1.config, {'labels': ['database']})
        self.assertEqual(metric2.type, 'histogram')
        self.assertEqual(metric2.description, 'metric two')
        self.assertEqual(
            metric2.config,
            {'labels': ['database'], 'buckets': [10, 100, 1000]})

    def test_load_queries_section(self):
        '''The 'queries section is loaded from the config file.'''
        config = {
            'databases': {
                'db1': {'dsn': 'dbname=foo'},
                'db2': {'dsn': 'dbname=bar'}},
            'metrics': {
                'm1': {'type': 'summary'},
                'm2': {'type': 'histogram'}},
            'queries': {
                'q1': {
                    'interval': 10,
                    'databases': ['db1'],
                    'metrics': ['m1'],
                    'sql': 'SELECT 1'},
                'q2': {
                    'interval': 10,
                    'databases': ['db2'],
                    'metrics': ['m2'],
                    'sql': 'SELECT 2'}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config))
        with open(config_file) as fd:
            result = load_config(fd)
        query1, query2 = sorted(result.queries, key=attrgetter('name'))
        self.assertEqual(query1.name, 'q1')
        self.assertEqual(query1.databases, ['db1'])
        self.assertEqual(query1.metrics, ['m1'])
        self.assertEqual(query1.sql, 'SELECT 1')
        self.assertEqual(query2.name, 'q2')
        self.assertEqual(query2.databases, ['db2'])
        self.assertEqual(query2.metrics, ['m2'])
        self.assertEqual(query2.sql, 'SELECT 2')

    def test_load_queries_unknown_databases(self):
        '''An error is raised if database names in query config are unknown.'''
        config = {
            'metrics': {'m': {'type': 'summary'}},
            'queries': {
                'q': {
                    'interval': 10,
                    'databases': ['db1', 'db2'],
                    'metrics': ['m'],
                    'sql': 'SELECT 1'}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(
            str(cm.exception), "Unknown databases for query 'q': db1, db2")

    def test_load_queries_unknown_metrics(self):
        '''An error is raised if metric names in query config are unknown.'''
        config = {
            'databases': {
                'db': {'dsn': 'dbname=foo'}},
            'queries': {
                'q': {
                    'interval': 10,
                    'databases': ['db'],
                    'metrics': ['m1', 'm2'],
                    'sql': 'SELECT 1'}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(
            str(cm.exception), "Unknown metrics for query 'q': m1, m2")

    def test_load_queries_missing_key(self):
        '''An error is raised if keys are missing in the queries section.'''
        config = {'queries': {'q1': {'interval': 10}}}
        config_file = self.tempdir.mkfile(content=yaml.dump(config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(
            str(cm.exception), "Missing key 'databases' for query 'q1'")

    def test_load_queries_interval_seconds(self):
        ''' The query interval is stored as seconds.'''
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with open(config_file) as fd:
            config = load_config(fd)
        [query] = config.queries
        self.assertEqual(query.interval, 10)

    def test_load_queries_interval_as_string(self):
        ''' The interval can be passed as string.'''
        self.config['queries']['q']['interval'] = '10'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with open(config_file) as fd:
            config = load_config(fd)
        [query] = config.queries
        self.assertEqual(query.interval, 10)

    def test_load_queries_interval_second_suffix(self):
        ''' The 's' suffix can be used in interval values for seconds.'''
        self.config['queries']['q']['interval'] = '10s'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with open(config_file) as fd:
            config = load_config(fd)
        [query] = config.queries
        self.assertEqual(query.interval, 10)

    def test_load_queries_interval_minute_suffix(self):
        ''' The 'm' suffix can be used in interval values for minutes.'''
        self.config['queries']['q']['interval'] = '10m'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with open(config_file) as fd:
            config = load_config(fd)
        [query] = config.queries
        self.assertEqual(query.interval, 600)

    def test_load_queries_interval_hour(self):
        ''' The 'h' suffix can be used in interval values for hours.'''
        self.config['queries']['q']['interval'] = '1h'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with open(config_file) as fd:
            config = load_config(fd)
        [query] = config.queries
        self.assertEqual(query.interval, 3600)

    def test_load_queries_interval_day(self):
        ''' The 'd' suffix can be used in interval values for days.'''
        self.config['queries']['q']['interval'] = '1d'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with open(config_file) as fd:
            config = load_config(fd)
        [query] = config.queries
        self.assertEqual(query.interval, 3600 * 24)

    def test_invalid_interval_suffix(self):
        '''An invalid suffix for query interval raises an error.'''
        self.config['queries']['q']['interval'] = '1x'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(str(cm.exception), "Invalid interval for query 'q'")

    def test_invalid_interval_not_number(self):
        '''An query interval that is not a number raises an error.'''
        self.config['queries']['q']['interval'] = 'wrong'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(str(cm.exception), "Invalid interval for query 'q'")

    def test_invalid_interval_not_integer(self):
        '''An query interval that is not an integer raises an error.'''
        self.config['queries']['q']['interval'] = '1.5m'
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(str(cm.exception), "Invalid interval for query 'q'")

    def test_invalid_interval_zero(self):
        '''An query interval of zero raises an error.'''
        self.config['queries']['q']['interval'] = 0
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(str(cm.exception), "Invalid interval for query 'q'")

    def test_invalid_interval_negative(self):
        '''An negative query interval raises an error.'''
        self.config['queries']['q']['interval'] = -20
        config_file = self.tempdir.mkfile(content=yaml.dump(self.config))
        with self.assertRaises(ConfigError) as cm, open(config_file) as fd:
            load_config(fd)
        self.assertEqual(str(cm.exception), "Invalid interval for query 'q'")
