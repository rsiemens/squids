SQuidS Documentation
====================


.. centered:: A Python library that makes it simple to produce and consume tasks using AWS SQS.

.. figure:: https://user-images.githubusercontent.com/8187804/166835620-151c9c59-25b5-45af-949a-e8123a3578dd.png

.. centered:: Icon made by `Freepik <https://www.freepik.com>`_ from `www.flaticon.com <https://www.flaticon.com/>`_

.. code-block:: python

    import squids

    app = squids.App("my-first-squids-app")

    @app.task(queue="squids-example")
    def greet(name):
        print(f"Hello {name}!")

    greet.send("World")
    consumer = app.create_consumer("squids-example")
    consumer.consume()
    # >> Hello World!

.. toctree::
   :maxdepth: 1

   installation
   quickstart
   guide
   api