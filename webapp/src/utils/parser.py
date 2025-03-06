import json
import argparse


def get_config():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--begin_date",
        type=str,
        help="The begin date for collection in format dd-mm-yyyy",
    )
    parser.add_argument(
        "--end_date", type=str, help="The end date for collection in format dd-mm-yyyy"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5,
        help="How much time to wait on a request connection",
    )
    parser.add_argument("-c", "--config", type=str, help="Config file")
    args = parser.parse_args()

    if args.config:
        config = json.load(open(args.config))
        parser.set_defaults(**config)
        args = parser.parse_args()

    return args
