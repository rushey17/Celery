import time
from datetime import datetime, timedelta
from celery import Celery
from celery.schedules import crontab
import random

app = Celery("tasks",
            backend="db+postgresql://postgres:postgres@localhost:5432/celery-postgres",
            broker='redis://localhost:6379/0')


CELERYBEAT_SCHEDULE = {
    'add-every-30-seconds': {
        'task': 'tasks.add',
        'schedule': crontab(minute='*/1')
    },
}

CELERY_TIMEZONE = 'UTC'

minute = 2

app.conf.beat_schedule = {
    'add-schedule': {
        'task': 'add',
        'schedule': crontab(minute=minute, hour=15),
        'args': (16, 2),
    },
    'divide-schedule': {
    'task': 'divide',
    'schedule': crontab(minute=25, hour=16),
    'args': (16, 8),
    },
    'multiply-schedule': {
        'task': 'multiply',
        'schedule': crontab(minute=12, hour=15),
        'args': (16, 16),
    }
}

app.conf.timezone = 'UTC'

@app.task(name='add',bind=True,default_retry_delay=5,max_retries=3)
def add(self, x, y):

	num = random.randint(1, 10)
	print(num)
	try:
		# If number is odd, fail the task
		if num % 2:
			raise Exception()
		# If number is even, succeed the task
		else:
			return x + y
	except Exception as e:
		self.retry(exc=e)

@app.task(name='divide')
def divide(x, y):
    time.sleep(5)
    return x / y

@app.task(name='schedule')
def schedule():
    app.conf.beat_schedule['multiply-schedule'] = {'task': 'multiply', 'schedule': crontab(minute=29, hour=16), 'args': (16, 8)}
    print(app.conf.beat_schedule)
    return 'cron started'

@app.task(name='multiply')
def multiply(x, y):
    time.sleep(5)
    return x * y

@app.task(name='print_result')
def print_result():
    print('something')
    return 'something'

@app.task(name='write_file')
def write_file():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    i = app.control.inspect()
    with open('jobs.txt', 'w') as f:
        f.write(str(i.scheduled()))
    
    return 'write completed'







