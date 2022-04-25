import time
import random
import os
from squids import App, Task

app = App('test')


@app.pre_send
def before_send(queue: str, body: str):
    print(f"Sending {body} to {queue}")


@app.post_send
def after_send(queue: str, body: str, response: dict):
    print(f"Got response {response}")


@app.pre_task
def before_task(task: Task):
    print(f'(pid: {os.getpid()}) Before task {task.name}({task.id})')


@app.post_task
def after_task(task: Task):
    print(f'After task {task.name}({task.id})')


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