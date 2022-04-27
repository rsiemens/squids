import argparse
import importlib
import os
import sys
from textwrap import dedent

from squids.consumer import run_loop


def banner():
    return """\

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


def parse_args():
    parser = argparse.ArgumentParser()
    # I'm not sure if I like processing many queues as long polling can kind
    # of mess it up in terms of consumption throughput
    parser.add_argument(
        "--queues",
        action="store",
        type=str,
        nargs="+",
        required=False,
        help=(
            'The names of the SQS queues to process: --queues "queue1 queue2 queue3". '
            "Defaults to all queues in the app if not provided.",
        ),
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
    return parser.parse_args()


def import_app(import_path):
    module, path = import_path.split(".", 1)
    mod = importlib.import_module(module)

    app = mod
    for part in path.split("."):
        app = getattr(app, part)

    return app


def run(args):
    app = import_app(args.app)

    if not args.queues:
        queues = list({t.queue for t in app._tasks.values()})
    else:
        queues = args.queues

    task_names = [n for n, t in app._tasks.items() if t.queue in queues]

    print(banner())
    print(
        "[config]\n"
        f"  app = {app.name}\n"
        f"  queues = {queues}\n"
        f"  workers = {args.workers}\n"
    )

    if not task_names:
        print(f'No tasks registered for queues "{queues}"', file=sys.stderr)
        return

    print("[tasks]")
    for name in task_names:
        print(f"  - {name}")

    run_loop(app, queues, args.workers)


run(parse_args())
