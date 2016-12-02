import asyncio
import datetime
from multiprocessing.pool import Pool
from aiohttp import ClientSession
import csv
import json

MAX_USERS = 1200000000
USER_URL = 'http://facebook.com/profile.php?id=%s'
GRAPH_URL = 'http://graph.facebook.com/%s/picture?width=100&height=100&redirect=false'

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
        with open('result%s.csv' % file_end, 'a') as csvfile:
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
    result = loop.run_until_complete(future)


def main():
    begin_date = datetime.datetime.now()
    print('BEGIN ' + str(begin_date))
    p = Pool(4)
    # begin end file_end
    print(p.map(threaded, [(0,1000,0),(1001,2000,1),(2001,3000,2),(3001,4000,3)]))
    print('TOTAL ' + str(datetime.datetime.now() - begin_date))
    print('END')


if __name__ == '__main__':
    main()
