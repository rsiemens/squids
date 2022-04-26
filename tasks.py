import time
import random
import os
from squids import App, Task

app = App('test', config={'endpoint_url': 'http://localhost:4566'})


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


@app.task(queue_name='test-queue')
def printer(message):
    print(f"Worker {os.getpid()} got message: {message}")


@app.task(queue_name='test-queue')
def long_running_email_task(to_addr, from_addr, body, headers=None):
    time.sleep(random.randint(0, 5))
    headers = {} if headers is None else headers
    print('Sending email:')
    if headers:
        for k, v in headers.items():
            print(f'{k}: {v}')
    print(f'to: {to_addr}\nfrom: {from_addr}\n\n{body}')