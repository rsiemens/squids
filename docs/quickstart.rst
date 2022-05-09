Quickstart
==========

The best way to learn how to use SQuidS is by building something. This section will have you
building and running a very simple example app to download all the github repos from the Django
orginization.

Let's get started!

The first thing we need to do is to create an SQS queue. You can follow the `AWS SQS docs <https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-getting-started.html#step-create-queue>`_ for this.
Or if you have the AWS cli installed already then you can simply do ``aws sqs create-queue --queue-name squids-example``.

Now we can start building our little application. Open up your editor of choice and let's start building.

.. code-block:: python

    import requests
    from squids import App

    app = App("squids-example-app")

    @app.task(queue="squids-example")
    def download_repo(repo, branch):
        with requests.get(
            f"https://github.com/django/{repo}/archive/refs/heads/{branch}.zip",
            stream=True
        ) as res:
            with open(f"{repo}.zip", "wb+") as f:
                for chunk in res.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Download for {repo} complete")

    if __name__ == "__main__":
        res = requests.get("https://api.github.com/orgs/django/repos")
        for repo in res.json():
            print(f"Sending download job for {repo['name']}")
            download_repo.send(repo["name"], repo["default_branch"])

Let's quickly go over what this does starting at the top.

.. code-block:: python

    app = App("squids-example-app")

This creates an instance of our SQuidS application, called ``squids-example-app``, which holds all the tasks we register and knows
how to communicate with SQS. It also takes an optional ``config`` keyword argument which is a dictionary that takes the same values as `boto3.session.Session.resource <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.resource>`_.

.. code-block:: python

    @app.task(queue="squids-example")
    def download(repo, branch):
        with requests.get(
            f"https://github.com/django/{repo}/archive/refs/heads/{branch}.zip",
            stream=True
        ) as res:
            with open(f"{repo}.zip", "wb+") as f:
                for chunk in res.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Download for {repo} complete")

Our first task! It's just an ordinary function which we register with our app by using the
:meth:`.App.task` decorator. The decorator takes a single argument, ``queue``, which is the name of the queue that
the task should go to when it's run. By decorating our function we can now send our task to the
``squids-example`` queue which can then be picked up and run by consumers (we'll get to consuming in a minute).

The body of the function is the expensive work that we want to avoid running synchronously in our
process. In this case it downloads a zip file from github and saves it to a file in the current
directory.

.. code-block:: python

    if __name__ == "__main__":
        res = requests.get("https://api.github.com/orgs/django/repos")
        for repo in res.json():
            print(f"Sending download job for {repo['name']}")
            download_repo.send(repo["name"], repo["default_branch"])

This is the entry point to our little script. It gets a list of all the repos in the Django
organization and for each repo sends the task to SQS for consumers to pick up.

Let's go ahead and run it. ::

    $ python example.py
    Sending download job for djangosnippets.org
    Sending download job for djangoproject.com
    Sending download job for djangobench
    ...

Nice! Our tasks have been sent to the squids-example queue, but now we need a way to consume and
run them. SQuidS includes a command line consumer which you can use to quickly start consuming tasks. ::

    $ squids --queue squids-example  --app example:app

      /######   /######            /##       /##  /######
     /##__  ## /##__  ##          |__/      | ## /##__  ##
    | ##  \__/| ##  \ ## /##   /## /##  /#######| ##  \__/
    |  ###### | ##  | ##| ##  | ##| ## /##__  ##|  ######
     \____  ##| ##  | ##| ##  | ##| ##| ##  | ## \____  ##
     /##  \ ##| ##/## ##| ##  | ##| ##| ##  | ## /##  \ ##
    |  ######/|  ######/|  ######/| ##|  #######|  ######/
     \______/  \____ ### \______/ |__/ \_______/ \______/
                    \__/

    [config]
      app = squids-example-app
      queue = squids-example
      workers = 8
      report-interval = 300
      polling-wait-time = 5

    [tasks]
      - example.download_repo

    Download for ticketbot complete
    Download for djangobench complete
    Download for djangosnippets.org complete
    ...


The command line consumer takes two required arguments, ``--queue`` and ``--app``. ``--queue`` is the
name of the queue it should be consuming tasks from and ``--app`` is the path to the :class:`squids.App`
instance which has all the tasks you registered with it.

The consumer will fetch messages from the queue and then send them to worker processes to run our
``download_repo`` function.  If you take a look at your directory you should see a bunch of ``zip``
files for all the repos we downloaded.

To stop the consumer hit ``Ctrl+C``.