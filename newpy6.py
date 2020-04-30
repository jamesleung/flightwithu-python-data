# coding=utf-8
import requests
import json
import time
#from apscheduler.schedulers.blocking import BlockingScheduler
import pika
import threading
import sys
if sys.version > '3':
    import queue as Queue
else:
    import Queue
import urllib3

#消除requests警告信息
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

credentials = pika.PlainCredentials('admin', 'admin')
parameters = pika.ConnectionParameters(host = '122.51.134.114',port = 5672,virtual_host = '/',credentials = credentials)
connection = pika.BlockingConnection(parameters)
channel=connection.channel()
result = channel.queue_declare(queue = 'python-test6')

# 设置队列长度
#workQueue = Queue.Queue(2000)
candidateUrls = Queue.Queue(2000)
# 真正有效数据队列
validateUrlQueue = Queue.Queue(100)
validateUrls = []
#realQueue = Queue.Queue(100)
# 线程池
#threads = []

# 用一个线程单独试探出有效url
# 将url填充到队列
def getAllAliadUrls():
    for i in range(0,2000):
        fileNum = (str(i)).rjust(4, '0')
        url = "https://tar1090.adsbexchange.com/data/globe_" + fileNum + ".json"
        candidateUrls.put(url)

class headUrlThread(threading.Thread):
    def __init__(self,q,func,s):
        threading.Thread.__init__(self)
        self.q = q
        self.func = func
        self.s = s
    def run(self):
        while True:
            try:
                self.func(self.q)
                time.sleep(self.s)
            except Exception as e:
                print(e)

def headUrl(q):    
    try:
        # 从队列里获取url
        url = q.get(timeout=2)
        # 保留有效url
        if (url in validateUrls):
            print("url is already exist...")
        else:
            r = requests.head(url)
            if r.status_code == 200:
                validateUrlQueue.put(url)
                validateUrls.append(url)
                #print("adding an new url..." + url)
    except Exception as e:
        print(e)

def getData(q):
    try:
        url = q.get(timeout=2)
        job(url)
        #print(url)
    except Exception as e:
        print(e)
    #重新归队，用于下次再试探
    q.put(url)

def job(url):
    try:
        headers = {
                'Content-Type': 'application/json;charset=utf-8',
                'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15'
                }
        r = requests.get(url, headers = headers, verify = False)        
        d = json.loads(r.text)
        aircrafts = d['aircraft']
        #print("published aircrafts: " + str(len(aircrafts)))
        for ac in aircrafts:
            try:
                message = json.dumps(ac)
                try:
                    channel.basic_publish(exchange = '',routing_key = 'python-test6',body = message)
                except pika.exceptions.ConnectionClosed:
                    connection = pika.BlockingConnection(parameters)
                    channel = connection.channel()
                    channel.basic_publish(exchange = '',routing_key = 'python-test6',body = message)
                except Exception as e:
                    connection = pika.BlockingConnection(parameters)
                    channel = connection.channel()
                    channel.basic_publish(exchange = '',routing_key = 'python-test6',body = message)
            except:
                continue        
    except Exception as e:
        print(e)

#初始化备选url
getAllAliadUrls()
#有效url
#创建新线程
for tId in range(0,10):
    thread = headUrlThread(candidateUrls, headUrl, 60)
    thread.start()
    #threads.append(thread)
'''
#等待所有线程完成
for t in threads:
    t.join()
'''
#获取数据
#创建新线程
for tId in range(0,10):
    thread = headUrlThread(validateUrlQueue, getData, 18)
    thread.start()


""" '''
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters(host = '122.51.134.114',port = 5672,virtual_host = '/',credentials = credentials))
channel=connection.channel()
result = channel.queue_declare(queue = 'python-test6')

def job(url):
    headers = {
            'Content-Type': 'application/json;charset=utf-8',
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15'
        }
    r = requests.get(url, headers = headers, verify = False)

    d = json.loads(r.text)

    aircrafts = d['aircraft']

    i = 0
    for ac in aircrafts:
        i += 1
        print(i)
        message = json.dumps(ac)
        channel.basic_publish(exchange = '',routing_key = 'python-test6',body = message)
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



class myThread(threading.Thread):
    def __init__(self,name,q,d):
        threading.Thread.__init__(self)
        self.name = name
        self.q = q
        self.d = d
    def run(self):
        while True:
            try:
                crawler(self.name,self.q,self.d)
            except:
                break
def crawler(threadName,q,d):
    if d == 0:
        # 从队列里获取url
        url = q.get(timeout=2)
        try:
            r = requests.head(url)
            # 打印：队列长度，线程名，响应吗，正在访问的url
            if r.status_code == 200:
                ''''''print(realWorkQueue.qsize(),threadName,r.status_code,url)''''''
                realWorkQueue.append(url)
        except Exception as e:
            print(q.qsize(),threadName,"Error: ",e)
        q.put(url)
    elif d == 1:
        # 从队列里获取url
        url = q.get(timeout=2)
        try:
            ''''''r = requests.get(url,timeout = 20)''''''
            headers = {
            'Content-Type': 'application/json;charset=utf-8',
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15'
            }
            r = requests.get(url, headers = headers, verify = False)

            d = json.loads(r.text)

            aircrafts = d['aircraft']

            i = 0
            for ac in aircrafts:
                i += 1
                message = json.dumps(ac)
                channel.basic_publish(exchange = '',routing_key = 'python-test6',body = message)
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
            # 打印：队列长度，线程名，响应吗，正在访问的url
        except Exception as e:
            print(q.qsize(),threadName,"Error: ",e)
        q.put(url)

class myThread1(threading.Thread):
    def __init__(self,name,q):
        threading.Thread.__init__(self)
        self.name = name
        self.q = q
    def run(self):
        while True:
            try:
                crawler1(self.name,self.q)
            except:
                break
def crawler1(threadName,q):
    # 从队列里获取url
    url = q.get(timeout=2)
    try:
        ''''''r = requests.get(url,timeout = 20)''''''
        headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15'
        }
        r = requests.get(url, headers = headers, verify = False)

        d = json.loads(r.text)

        aircrafts = d['aircraft']

        i = 0
        for ac in aircrafts:
            i += 1
            message = json.dumps(ac)
            channel.basic_publish(exchange = '',routing_key = 'python-test6',body = message)
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
        # 打印：队列长度，线程名，响应吗，正在访问的url
    except Exception as e:
        print(q.qsize(),threadName,"Error: ",e)
    q.put(url)

#创建新线程
for tId in range(0,99):
    thread = myThread(tId,workQueue,0)
    thread.start()
    threads.append(thread)
#将url填充到队列
for i in range(0,1000):
    fileNum = (str(i)).rjust(4, '0')
    url = "https://tar1090.adsbexchange.com/data/globe_" + fileNum + ".json"
    workQueue.put(url)
#等待所有线程完成
for t in threads:
    t.join()
    
#创建新线程
for tId in range(0,49):
    thread = myThread1(tId,realQueue)
    thread.start()
    threads1.append(thread)
i = 0

''''''
while True:
    realQueue.put(realWorkQueue[i])
    if i == len(realWorkQueue) - 1:
        i = 0
    else:
        i += 1
''''''
for item in realWorkQueue:
    realQueue.put(item)
    time.sleep(5)
#等待所有线程完成
for t in threads1:
    t.join()
'''

'''
if __name__ == '__main__':
    scheduler = BlockingScheduler({'apscheduler.job_defaults.max_instances':'2'})
    scheduler.add_job(job, 'interval', seconds=11)
    scheduler.start()
    ''' """