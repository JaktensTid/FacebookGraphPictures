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
    def add_work_parts(chunk):
        """Add to the every table parts for threading"""
        cfrom = chunk['from']
        cto = chunk['to']
        count = cto - cfrom
        parts = cpu_count()
        balance = 0
        working_size = int((cto - cfrom) / parts)
        if working_size > MAX_CHUNK_LENGTH:
            working_size = MAX_CHUNK_LENGTH
            parts = int(count / working_size)
        if (cto - cfrom) % parts != 0:
            balance = cto - (working_size * parts)
        creator = lambda b, e: (b, e, str(b) + '-' + str(e))
        chunk['parts'] = [creator(int((cfrom + working_size * i) + 1), int(cfrom + working_size * (i + 1)))
                  for i in range(parts)] + [creator(cto - balance + 1 + cfrom, cto)] if balance != 0 else []
        return chunk
    chunks = []
    #Distribute input begin and end to tables
    for table in tables:
        if range_contains(table['range'][0], table['range'][1], begin):
            chunk = {'tname':table['name'],
                           'from':table['max'] - (table['range'][1] - begin),
                           'to':table['max'] if end > table['range'][1] else end}
            chunks.append(add_work_parts(chunk))
            continue
        if range_contains(table['range'][0], table['range'][1], end):
            chunk = {'tname': table['name'],
                           'from': 0,
                           'to': end - table['range'][0]}
            chunks.append(add_work_parts(chunk))
            continue
        chunk = {'tname':table['name'],
                       'from':0,
                       'to':table['max']}
        chunks.append(add_work_parts(chunk))

    return chunks

def range_contains(f, t, number):
    return f <= number <= t

def is_error(tables,begin,end):
    summa = sum([table['max'] for table in tables])
    if begin > summa or end > summa:
        print('ERROR: Max id should be less than %s of faces on the website' % int(summa))
        return True
    if begin == end:
        print('ERROR: Begin = end')
        return True
    if begin < 0 or end < 0:
        print("ERROR: Ids can't be less, that 0")


def main(begin, end):
    tables = get_tables()

    if(is_error(tables,begin,end)):
        return

    print('Beginning in: ' + str(datetime.now()))

    # Swap begin and end, if begin > end
    if begin > end:
        t = end
        end = begin
        begin = t

    covering_tables = [table for table in tables
                 if (table['range'][0] > begin and table['range'][1] < end)
                       or range_contains(table['range'][0], table['range'][1], begin)
                       or range_contains(table['range'][0], table['range'][1], end)]

    chunks = get_chunks(covering_tables,begin,end)
    #p = Pool(cpu_count())
    #p.map(threaded, chunks)
    print('ENDED PROCESSING OF ' + str(end - begin) + ' URLS')


def test(chunks,begin,end):
    assert sum([chunk['to'] for chunk in chunks]) - sum([chunk['from'] for chunk in chunks]) == end - begin

if __name__ == '__main__':
    main(1, 1000000000)
