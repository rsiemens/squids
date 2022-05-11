import argparse
import importlib
import logging
import os
import sys
from textwrap import dedent

from squids.consumer import run_loop


def banner():
    return dedent(
        """\

                /######   /######            /##       /##  /######
               /##__  ## /##__  ##          |__/      | ## /##__  ##
              | ##  \__/| ##  \ ## /##   /## /##  /#######| ##  \__/
              |  ###### | ##  | ##| ##  | ##| ## /##__  ##|  ######
               \____  ##| ##  | ##| ##  | ##| ##| ##  | ## \____  ##
               /##  \ ##| ##/## ##| ##  | ##| ##| ##  | ## /##  \ ##
              |  ######/|  ######/|  ######/| ##|  #######|  ######/
               \______/  \____ ### \______/ |__/ \_______/ \______/
                              \__/
    """
    )


def parse_args():
    parser = argparse.ArgumentParser()
    # I'm not sure if I like processing many queues as long polling can kind
    # of mess it up in terms of consumption throughput
    parser.add_argument(
        "-q",
        "--queue",
        action="store",
        type=str,
        required=True,
        help="The name of the SQS queue to process.",
    )
    parser.add_argument(
        "-w",
        "--workers",
        action="store",
        type=int,
        required=False,
        default=os.cpu_count() or 2,
        help="The number of workers to run. Defaults to the number of CPUs in the system",
    )
    parser.add_argument(
        "-a",
        "--app",
        action="store",
        type=str,
        required=True,
        help="Path to the application class something like package.module:app where app is an instance of squids.App",
    )
    parser.add_argument(
        "--report-interval",
        action="store",
        type=int,
        required=False,
        default=300,
        help=(
            "How often to call the report_queue_stats callback with GetQueueAttributes for the queue in seconds. "
            "Defaults to 300 (5min). If no report_queue_stats callback has been registered then GetQueueAttributes "
            "will not be requested. The report-interval is an at earliest time. It may take longer depending on"
            "the polling-wait-time."
        ),
    )
    parser.add_argument(
        "--polling-wait-time",
        action="store",
        type=int,
        required=False,
        choices=range(0, 21),
        default=5,
        help=(
            "The WaitTimeSeconds for polling for messages from the queue. Consult the AWS SQS docs on long polling "
            "for more information about this setting. "
            "https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling"
        ),
    )
    parser.add_argument(
        "--visibility-timeout",
        action="store",
        type=int,
        required=False,
        default=30,
        help=(
            "The VisibilityTimeout duration (in seconds) that the received messages are hidden from subsequent "
            "retrieve requests after being retrieved by a ReceiveMessage request."
        ),
    )
    parser.add_argument(
        "--log-level",
        action="store",
        type=str,
        required=False,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level for the consumer. Logs will be handled using the logging.SteamHandler with the stream set to stdout",
    )
    return parser.parse_args()


def import_app(import_path):
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    module, path = import_path.split(":", 1)
    mod = importlib.import_module(module)

    app = mod
    for part in path.split("."):
        app = getattr(app, part)

    return app


def configure_logger(level):
    logger = logging.getLogger("squidslog")
    logger.setLevel(getattr(logging, level))
    logger.addHandler(logging.StreamHandler(stream=sys.stdout))


def run(args):
    configure_logger(args.log_level)
    app = import_app(args.app)
    task_names = [n for n, t in app._tasks.items() if t.queue == args.queue]

    print(banner())
    print(
        "[config]\n"
        f"  app = {app.name}\n"
        f"  queue = {args.queue}\n"
        f"  workers = {args.workers}\n"
        f"  report-interval = {args.report_interval}\n"
        f"  polling-wait-time = {args.polling_wait_time}\n"
        f"  visibility-timeout = {args.visibility_timeout}\n"
        f"  log-level = {args.log_level}\n"
    )

    if not task_names:
        print(f'No tasks registered for queue "{args.queue}"', file=sys.stderr)
        return

    print("[tasks]")
    for name in task_names:
        print(f"  - {name}")
    print()

    run_loop(
        app,
        args.queue,
        args.workers,
        args.report_interval,
        args.polling_wait_time,
        args.visibility_timeout,
    )


def main():
    run(parse_args())
