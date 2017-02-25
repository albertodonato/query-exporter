from functools import partial
import asyncio
from datetime import datetime
import random

from aiohttp import web
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    CONTENT_TYPE_LATEST,
    generate_latest)
from toolrack.async import PeriodicCall


def create_metrics(registry):
    '''Create metrics.'''
    return {'metric': Gauge('metric', 'sample metric', registry=registry)}


def create_web_app(loop, registry, periodic_call):
    '''Create an aiohttp web application to export metrics.'''
    app = web.Application(loop=loop)
    app.router.add_get('/', _home)
    app.router.add_get('/metrics', partial(_metrics, registry))

    def start_periodic(_):
        periodic_call.start(10)

    async def stop_periodic(_):
        await periodic_call.stop()

    app.on_startup.append(start_periodic)
    app.on_shutdown.append(stop_periodic)
    return app


async def _home(request):
    '''Home page request handler.'''
    return web.Response(body=b'Export metrics from SQL queries.')

async def _metrics(registry, request):
    '''Handler for metrics.'''
    body = generate_latest(registry)
    response = web.Response(body=body)
    response.content_type = CONTENT_TYPE_LATEST
    return response


def _loop(loop, metrics):
    print('>>', datetime.now())
    loop.create_task(_loop2(metrics))

async def _loop2(metrics):
    from .db import DataBase, Query
    q = Query('test-query', 20, ['metric'], 'SELECT random() * 1000')
    async with DataBase('db', 'dbname=ack') as db:
        results = await db.execute(q)
        for metric, value in results.items():
            metrics[metric].set(value)


def main():
    loop = asyncio.get_event_loop()

    registry = CollectorRegistry(auto_describe=True)
    metrics = create_metrics(registry)

    periodic_call = PeriodicCall(loop, _loop, loop, metrics)
    app = create_web_app(loop, registry, periodic_call)
    web.run_app(app, port=9090)
