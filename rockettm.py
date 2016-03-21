from __future__ import print_function
from pika import BlockingConnection, ConnectionParameters
import json


class tasks(object):
    subs = {}
    logger = print
    queues = []
    ip = "localhost"
    conn = False
    channel = False

    @staticmethod
    def connect(ip=None):
        if ip:
            tasks.ip = ip
        tasks.conn = BlockingConnection(ConnectionParameters(tasks.ip))
        tasks.channel = tasks.conn.channel()

    @staticmethod
    def add_task(event, func):
        tasks.logger(event)
        if event not in tasks.subs:
            tasks.subs[event] = []
        tasks.subs[event].append(func)

    @staticmethod
    def task(event):
        def wrap_function(func):
            tasks.add_task(event, func)
            return func
        return wrap_function

    @staticmethod
    def send_task(queue, event, *args):
        if not tasks.channel or tasks.channel.is_closed:
            tasks.connect()
        if queue not in tasks.queues:
            try:
                tasks.channel.queue_declare(queue=queue, passive=True)
                tasks.queues.append(queue)
            except:
                raise Exception("Queue not declare, first start the server")

        tasks.channel.basic_publish(exchange='',
                                    routing_key=queue,
                                    body=json.dumps({'event': event,
                                                     'args': args}))
# avoids having to import tasks
connect = tasks.connect
send_task = tasks.send_task
add_task = tasks.add_task
task = tasks.task
