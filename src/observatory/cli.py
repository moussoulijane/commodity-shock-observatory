import argparse
import asyncio
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Commodity Shock Observatory")
    sub = parser.add_subparsers(dest="command", required=True)
    fetch = sub.add_parser("fetch", help="Download public price histories with manifests")
    fetch.add_argument("--start", default="2000-01-01")
    fetch.add_argument("--end")
    sub.add_parser("init", help="Create database schema and Kafka topics")
    replay = sub.add_parser("replay")
    replay.add_argument("--path", type=Path, default=Path("data/quotes.jsonl"))
    replay.add_argument("--rate", type=float, default=0, help="Events/sec; 0 = fastest")
    replay.add_argument("--start")
    replay.add_argument("--end")
    macro = sub.add_parser("macro")
    macro.add_argument("--strict", action="store_true")
    macro.add_argument("--origins", type=int, default=96)
    sub.add_parser("evaluate-detectors")
    sub.add_parser("verify")
    sub.add_parser("reliability")
    load = sub.add_parser("load-test")
    load.add_argument("--events", type=int, default=5000)
    live = sub.add_parser("live")
    live.add_argument("--symbols", default="XAU/USD")
    args = parser.parse_args()
    if args.command == "fetch":
        from observatory.sources import fetch_history

        fetch_history(args.start, args.end)
    elif args.command == "init":
        from observatory.producer import create_topics
        from observatory.store import init

        init()
        create_topics()
    elif args.command == "replay":
        from observatory.producer import replay

        print({"published": replay(args.path, args.rate, args.start, args.end)})
    elif args.command == "macro":
        from observatory.macro import evaluate, fetch_macro

        evaluate(fetch_macro(args.strict), args.strict, max_origins=args.origins)
    elif args.command in ("verify", "reliability", "load-test", "evaluate-detectors"):
        from observatory import validation

        commands = {
            "verify": validation.verify,
            "reliability": validation.reliability,
            "load-test": lambda: validation.load_test(args.events),
            "evaluate-detectors": validation.evaluate_detectors,
        }
        commands[args.command]()
    elif args.command == "live":
        from observatory.producer import live

        asyncio.run(live(args.symbols.split(",")))


if __name__ == "__main__":
    main()
