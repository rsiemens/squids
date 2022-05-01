import argparse
import importlib
import os
import sys
from textwrap import dedent

from squids.consumer import run_loop


def banner():
    return dedent(
        """\
                                        .%%%:
                                      -%%%:#%%=
              .%%%:                  %%-%.::%+#%                  .%%%:
            -%%%:#%%=              =%::% ::::% :%*              -%%%:#%%=
           %%-%.::%+#%            #%.:%=:::::-% :%%            %%-%.::%+#%
         =%::% ::::% :%*         %%::-% ::::::%-::#%         =%::% ::::% :%*
        #%.:%=:::::-% :%%       %#:::%.::::::::% :-+%       #%.:%=:::::-% :%%
       %%::-% ::::::%-::#%     #%.:::# ::::::::#-:::##     %%::-% ::::::%-::#%
      %#:::%.::::::::% :-+%    % :::%.::::::::::% :::%    %#:::%.::::::::% :-+%
     #%.:::# ::::::::#-:::##  ++ .::% ::-:::-:::% :. :#  #%.:::# ::::::::#-:::##
     % :::%.::::::::::% :::%   %#%+*#::%#%:%%%::=%+%%%   % :::%.::::::::::% :::%
    ++ .::% ::-:::-:::% :. :#     -% ::%.#-%.%:::#=     ++ .::% ::-:::-:::% :. :#
     %#%+*#::%#%:%%%::=%+%%%       % ::##%:%%%:::%       %#%+*#::%#%:%%%::=%+%%%
        -% ::%.#-%.%:::#=          #. .:::::::::.%          -% ::%.#-%.%:::#=
         % ::##%:%%%:::%            #%*-.   .:*%%            % ::##%:%%%:::%
         #. .:::::::::.%             .-#%%%%%#-              #. .:::::::::.%
          #%*-.   .:*%%                                       #%*-.   .:*%%
           .-#%%%%%#-              :%%#  %##  ##%=             .-#%%%%%#-
                                   % :% #+:-% %::%
         :%%#  %##  ##%=           % :%:%:::% %.:#           :%%#  %##  ##%=
         % :% #+:-% %::%           %%#%  %#%. %%#%           % :% #+:-% %::%
         % :%:%:::% %.:#                                     % :%:%:::% %.:#
         %%#%  %#%. %%#%                                     %%#%  %#%. %%#%

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
        "--queue",
        action="store",
        type=str,
        required=True,
        help="The name of the SQS queue to process.",
    )
    parser.add_argument(
        "--workers",
        action="store",
        type=int,
        required=False,
        default=os.cpu_count() or 2,
        help="The number of workers to run. Defaults to the number of CPUs in the system",
    )
    parser.add_argument(
        "--app",
        action="store",
        type=str,
        required=True,
        help="Path to the application class something like module.app where app is an instance of squids.App",
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
    return parser.parse_args()


def import_app(import_path):
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    module, path = import_path.split(".", 1)
    mod = importlib.import_module(module)

    app = mod
    for part in path.split("."):
        app = getattr(app, part)

    return app


def run(args):
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
    )

    if not task_names:
        print(f'No tasks registered for queue "{args.queue}"', file=sys.stderr)
        return

    print("[tasks]")
    for name in task_names:
        print(f"  - {name}")
    print()

    run_loop(
        app, args.queue, args.workers, args.report_interval, args.polling_wait_time
    )


def main():
    run(parse_args())
