import random
import asyncio
import datetime
from multiprocessing.pool import Pool
from aiohttp import ClientSession
import csv

MAX_USERS = 1200000000
GRAPH_URL = 'http://graph.facebook.com/%s/picture?width=100&height=100&redirect=false'

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


async def bound_fetch(sem, url, session):
    async with sem:
       return await fetch(url, session)


async def run(begin, end):
    tasks = []
    sem = asyncio.Semaphore(50)
    async with ClientSession() as session:
        for i in range(begin,end):
            task = asyncio.ensure_future(bound_fetch(sem, GRAPH_URL % i, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses
        return responses


def threaded(t):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(t[0],t[1]))
    result = loop.run_until_complete(future)


def main():
    begin_date = datetime.datetime.now()
    print('BEGIN ' + str(begin_date))
    p = Pool(4)
    print(p.map(threaded, [(0,1000),(1001,2000),(2001,3000),(3001,4000)]))
    print('TOTAL ' + str(datetime.datetime.now() - begin_date))
    print('END')


if __name__ == '__main__':
    main()
