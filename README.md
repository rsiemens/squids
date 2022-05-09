<h1 align="center">
  SQuidS  
</h1>
<p align="center">
  <img src="https://github.com/rsiemens/squids/actions/workflows/ci.yml/badge.svg?branch=main" />
</p>
<p align="center">
  A Python library that makes it simple to produce and consume tasks using AWS SQS.
</p>
<p align="center">
  <img src="https://user-images.githubusercontent.com/8187804/166835620-151c9c59-25b5-45af-949a-e8123a3578dd.png" />
</p>
<p align="center">
  Icon made by <a href="https://www.freepik.com" title="Freepik">Freepik</a> from <a href="https://www.flaticon.com/" title="Flaticon">www.flaticon.com</a>
</p>

```python
import squids

app = squids.App("my-first-squids-app")

@app.task(queue="squids-example")
def greet(name):
    print(f"Hello {name}!")

greet.send("World")
consumer = app.create_consumer("squids-example")
consumer.consume()
# >> Hello World!
```

Installation
------------

`python -m pip install SQuidS`

Documentation
-------------

https://squids.readthedocs.io