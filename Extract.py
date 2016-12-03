import asyncio
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from aiohttp import ClientSession
import csv
import json
from datetime import datetime
import requests

MAX_USERS = 1200000000
USER_URL = 'http://facebook.com/%s'
GRAPH_URL = 'http://graph.facebook.com/%s/picture?width=100&height=100&redirect=false'
SELECT_MANY_URL = 'http://app.thefacesoffacebook.com/php/select_many_fbid.php?'
GET_INFO_FROM_TABLES_URL = 'http://app.thefacesoffacebook.com/php/get_info_from_tables.php'
MAX_CHUNK_LENGTH = 10000


def get_tables():
    # Facebook faces ids divided by tables. We have to get them
    response = requests.get(GET_INFO_FROM_TABLES_URL)
    text = response.text
    chunks = text.split(' ')
    creator = lambda splitted: {'name': splitted[0], 'max': int(splitted[1])}
    chunks = [creator(chunk.split(',')) for chunk in chunks]
    for i in range(len(chunks)):
        chunks[i]['range'] = (sum([r['max'] for r in chunks[0:i]]),chunks[i]['max'] + sum([r['max'] for r in chunks[0:i]]))
    return chunks


async def fetch(url, id, session):
    async with session.get(url) as response:
        return await response.read(), id


async def bound_fetch(sem, url, id, session):
    async with sem:
        return await fetch(url, id, session)


async def run(begin, end, file_end):
    tasks = []
    sem = asyncio.Semaphore(1000)
    async with ClientSession() as session:
        for id in range(begin, end):
            task = asyncio.ensure_future(bound_fetch(sem, GRAPH_URL % id, id, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses
        with open('%s Facebook faces .csv' % file_end, 'a') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['UserID', 'AvatarURL'])
            for response in responses._result:
                j = json.loads(response[0].decode('utf-8'))
                if 'error' not in j and not j['is_silhouette']:
                    spamwriter.writerow([USER_URL % response[1], j['data']['url']])


def threaded(t):
    # Use all cores of CPU + multithreaded requests
    print('Thread started. Begin: ' + str(t[0]) + ' - End: ' + str(t[1]))
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(t[0], t[1], t[2]))
    loop.run_until_complete(future)

def get_chunks(tables, begin, end):
    chunks = []
    for table in tables:
        if range_contains(table['range'][0], table['range'][1], begin):
            chunks.append({'tname':table['name'],'from':table['range'][1] - begin,'to':table['range'][0]})

def range_contains(f, t, number):
    return f <= number <= t

def main(begin, end):
    tables = get_tables()
    summa = sum([table['max'] for table in tables])
    print('Total number of faces on the website: ' + str(summa))

    if begin > summa or end > summa:
        print('ERROR: Max id should be less than %s of faces on the website' % int(summa))
        return
    print('Beginning in: ' + str(datetime.now()))

    # Swap begin and end, if begin > end
    if begin > end:
        t = end
        end = begin
        begin = t

    count = end - begin
    parts = cpu_count()
    balance = 0
    chunk_size = int((end - begin) / parts)

    if chunk_size > MAX_CHUNK_LENGTH:
        chunk_size = MAX_CHUNK_LENGTH
        parts = int(count / chunk_size)

    if (end - begin) % parts != 0:
        balance = end - (chunk_size * parts)

    covering_tables = [table for table in tables
                 if (table['range'][0] > begin and table['range'][1] < end)
                       or range_contains(table['range'][0], table['range'][1], begin)
                       or range_contains(table['range'][0], table['range'][1], end)]



    #p = Pool(cpu_count())
    # begin end file_end
    #p.map(threaded, chunks)
    print('ENDED PROCESSING OF ' + str(end - begin) + ' URLS')




if __name__ == '__main__':
    main(11, 18353243)
