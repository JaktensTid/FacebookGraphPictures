import asyncio
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from aiohttp import ClientSession
import csv
import json
import py_compile
py_compile.compile('Extract.py')

MAX_USERS = 1200000000
USER_URL = 'http://facebook.com/profile.php?id=%s'
GRAPH_URL = 'http://graph.facebook.com/%s/picture?width=100&height=100&redirect=false'
MAX_CHUNK_LENGTH = 100000
MIN_MULTIPROCESSED_LENGTH = 400000

async def fetch(url, id, session):
    async with session.get(url) as response:
        return await response.read(),id


async def bound_fetch(sem, url, id, session):
    async with sem:
       return await fetch(url, id, session)


async def run(begin, end, file_end):
    tasks = []
    sem = asyncio.Semaphore(50)
    async with ClientSession() as session:
        for id in range(begin,end):
            task = asyncio.ensure_future(bound_fetch(sem, GRAPH_URL % id, id, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses
        with open('Facebook faces %s.csv' % file_end, 'a') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['UserID','AvatarURL'])
            for response in responses._result:
                j = json.loads(response[0].decode('utf-8'))
                if 'error' not in j:
                    spamwriter.writerow([USER_URL % response[1],j['data']['url']])


def threaded(t):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(t[0],t[1],t[2]))
    loop.run_until_complete(future)


def main(begin, end):
    if begin > end:
        t = end
        end = begin
        begin = t
    count = end - begin
    parts = cpu_count()
    if count > MIN_MULTIPROCESSED_LENGTH:
        balance = 0
        chunk_size = int((end - begin)/parts)
        if chunk_size > MAX_CHUNK_LENGTH:
            chunk_size = MAX_CHUNK_LENGTH
            parts = int(count/chunk_size)
        if (end - begin) % parts != 0:
            balance = end - (chunk_size * parts)
        creator = lambda b,e: (b,e,str(b) + '-' + str(e))
        chunks = [creator(int((begin + chunk_size * i) + 1),int(begin + chunk_size * (i+1)))
                  for i in range(parts)] + [creator(end - balance + 1 + begin, end)] if balance != 0 else []
        p = Pool(cpu_count())
        # begin end file_end
        p.map(threaded, chunks)
    else:
        threaded((begin,end,str(begin) + '-' + str(end)))
    print('ENDED PROCESSING OF ' + str(end - begin) + ' URLS')


if __name__ == '__main__':
    main(11,18353243)
