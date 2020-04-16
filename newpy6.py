import requests
import json
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import pika

credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters(host = '122.51.134.114',port = 5672,virtual_host = '/',credentials = credentials))
channel=connection.channel()
result = channel.queue_declare(queue = 'python-test6')

def job():
    headers = {
            'Content-Type': 'application/json;charset=utf-8',
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15'
        }
    r = requests.get('https://tar1090.adsbexchange.com/data/globe_0011.json', headers = headers, verify = False)

    d = json.loads(r.text)

    aircrafts = d['aircraft']

    i = 0
    for ac in aircrafts:
        i += 1
        print(i)
        message = json.dumps(ac)
        channel.basic_publish(exchange = '',routing_key = 'python-test6',body = message)
        '''
        try:
            print(ac['hex'], end='  ')
            print(ac['flight'], end='  ')
            print(ac['gs'], end='  ')
            print(ac['alt_baro'], end='  ')
            print(ac['alt_geom'], end='  ')
            print(ac['squawk'], end='  ')
            print(ac['category'], end='  ')
            print(ac['lat'], end='  ')
            print(ac['lon'], end='  ')
            print('')
        except:
            continue
        '''



if __name__ == '__main__':
    scheduler = BlockingScheduler({'apscheduler.job_defaults.max_instances':'2'})
    scheduler.add_job(job, 'interval', seconds=11)
    scheduler.start()
