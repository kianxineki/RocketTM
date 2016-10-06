import sys


ip = "localhost"
port = 61613

user = 'guest'
password = 'guest'

logger = {  # 'filename': "rockettm.log",  # optional,
    # is not defined print in console
    'level': 10,  # optional
    'stream': sys.stdout
}

# search @task in imports
imports = ['examples.test1',
           'examples.test2',
           'examples.test3',
           'examples.test4']

# support params
# name (mandatory), string
# concurrency (mandatory), int
# durable (optional), boolean
# max_time (in seconds) (optional), int

queues = [{'name': 'rocket1', 'durable': True, 'concurrency': 10}]
#          {'name': 'rocket2', 'durable': True, 'concurrency': 1},
#          {'name': 'rocket3', 'durable': True, 'concurrency': 2,
#           'max_time': 1}]
