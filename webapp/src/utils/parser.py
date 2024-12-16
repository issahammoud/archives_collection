import json
import argparse
from src.helpers.enum import Archives


def get_config():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--archives",
        type=str,
        nargs="+",
        help=f"The archives names to collect from. One of {Archives}",
    )
    parser.add_argument(
        "--begin_date",
        type=str,
        help="The begin date for collection in format dd-mm-yyyy",
    )
    parser.add_argument(
        "--end_date", type=str, help="The end date for collection in format dd-mm-yyyy"
    )
    parser.add_argument(
        "--workers", type=int, default=16, help="How many workers to use"
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

    assert all(
        [archive in list(Archives) for archive in args.archives]
    ), "some archive name is not in the list"

    return args
