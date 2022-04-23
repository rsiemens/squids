import argparse
import importlib
import os

from squids import run_loop


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--queue",
        action="store",
        type=str,
        required=True,
        help="The name of the SQS queue to process",
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
    print(f"Workers {args.workers}")
    print(f"Processing queue {args.queue}")

    app = import_app(args.app)
    run_loop(app, args.queue, args.workers)


run(parse_args())
