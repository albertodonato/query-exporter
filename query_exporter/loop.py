from toolrack.async import PeriodicCall

from .db import DataBaseError, InvalidResultCount


class QueryLoop:
    '''Periodically performs queries.'''

    def __init__(self, config, metrics, logger, loop, interval=10):
        self._setup(config)
        self.loop = loop
        self.logger = logger
        self.interval = interval
        self._metrics = metrics
        self._periodic_call = PeriodicCall(self.loop, self._call)

    def start(self):
        '''Start the periodic check.'''
        self._periodic_call.start(self.interval)

    async def stop(self):
        '''Stop the periodic check.'''
        await self._periodic_call.stop()

    def _setup(self, config):
        self._metric_configs = {
            metric_config.name: metric_config
            for metric_config in config.metrics}
        self._databases = {
            database.name: database for database in config.databases}
        self._queries = config.queries
        # Track last execution time of each query on each database
        keys = (
            (query.name, db)
            for query in self._queries for db in query.databases)
        self._queries_db_last_time = dict.fromkeys(keys, 0)

    def _call(self):
        '''The periodic task function.'''
        now = self.loop.time()
        for query in self._queries:
            name = query.name
            for dbname in query.databases:
                last_time = self._queries_db_last_time[name, dbname]
                if not last_time or last_time + query.interval <= now:
                    self.loop.create_task(self._run_query(query, dbname))

    async def _run_query(self, query, dbname):
        ''''Run a Query on a DataBase.'''
        self.logger.debug(
            "running query '{}' on database '{}'".format(query.name, dbname))
        try:
            async with self._databases[dbname].connect() as conn:
                results = await conn.execute(query)
        except DataBaseError as error:
            self._log_db_error(query.name, error)
            return
        except InvalidResultCount as error:
            self._log_query_error(query.name, error)
            return

        for name, values in results.items():
            for value in values:
                self._update_metric(name, value, dbname)
        self._queries_db_last_time[(query.name, dbname)] = self.loop.time()

    def _log_query_error(self, name, error):
        '''Log an error related to database query.'''
        prefix = "query '{}' failed:".format(name)
        self.logger.error('{} {}'.format(prefix, error))

    def _log_db_error(self, name, error):
        '''Log a failed database query.'''
        self._log_query_error(name, error)
        for line in error.details:
            self.logger.debug(line)

    def _update_metric(self, name, value, dbname):
        '''Update value for a metric.'''
        metric_methods = {
            'counter': 'inc',
            'gauge': 'set',
            'histogram': 'observe',
            'summary': 'observe'}
        method = metric_methods[self._metric_configs[name].type]
        if value is None:
            # don't fail is queries that count return NULL
            value = 0.0
        self.logger.debug(
            "updating metric '{}': {} {}".format(name, method, value))
        metric = self._metrics[name].labels(database=dbname)
        getattr(metric, method)(value)
