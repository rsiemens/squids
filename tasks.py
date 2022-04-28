import time
import random
import os
from squids.core import App, Task

app = App('test', config={'endpoint_url': 'http://localhost:4566'})


@app.report_queue_stats
def reporter(queue: str, queue_stats: dict):
    print(f"Queue stats for {queue}")
    print(f"\tAvailable: {queue_stats['Attributes']['ApproximateNumberOfMessages']}")
    print(f"\tDelayed: {queue_stats['Attributes']['ApproximateNumberOfMessagesDelayed']}")
    print(f"\tIn flight: {queue_stats['Attributes']['ApproximateNumberOfMessagesNotVisible']}")

@app.pre_send
def before_send(queue: str, body: str):
    print("Running before send hook")


@app.post_send
def after_send(queue: str, body: str, response: dict):
    print("Running after send hook")


@app.pre_task
def before_task(task: Task):
    print("Running before task hook")


@app.post_task
def after_task(task: Task):
    print("Running after task hook")


@app.task(queue='test')
def printer(message):
    print(f"Worker {os.getpid()} got message: {message}")


@app.task(queue='test')
def long_running_email_task(to_addr, from_addr, body, headers=None):
    time.sleep(random.randint(0, 5))
    headers = {} if headers is None else headers
    print('Sending email:')
    if headers:
        for k, v in headers.items():
            print(f'{k}: {v}')
    print(f'to: {to_addr}\nfrom: {from_addr}\n\n{body}')


@app.task(queue='other')
def other_queue_task():
    print("I came from other-queue")


class MyTask(Task):
    queue = 'other'
    def run(self, some_arg):
        print(f"I'm from a custom task class: {some_arg}")


# opt for not doing magic to support a `Task.send` class method
my_task_instance = app.add_task(MyTask)


@app.task(queue='special')
def recursive_task(n):
    if n <= 0:
        print("I'm the last recursive task!")
    else:
        print(f"Running recursive task {n}")
        recursive_task.send(n-1)