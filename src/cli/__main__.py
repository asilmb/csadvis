"""Enables `python -m cli` invocation."""
import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cli", description="CS2 Analytics Platform CLI")
    subparsers = parser.add_subparsers(dest="command")

    # app: API + Dashboard
    subparsers.add_parser("start", help="Start API + Dashboard")

    # worker: Celery worker (optionally with beat scheduler)
    worker_parser = subparsers.add_parser("worker", help="Start Celery worker")
    worker_parser.add_argument(
        "--workers", type=int, default=2, help="Number of worker processes"
    )
    worker_parser.add_argument(
        "--beat", action="store_true", help="Also run the beat scheduler (single-process)"
    )

    # status: DB statistics
    subparsers.add_parser("status", help="Show database and system status")

    # monitor: Worker Registry + Task Queue
    subparsers.add_parser("monitor", help="Show worker registry and task queue stats")

    # watchdog: reclaim stuck tasks
    subparsers.add_parser("watchdog", help="Run stuck-task watchdog once")

    # validate-prices: compare DB prices against Steam Market API
    vp_parser = subparsers.add_parser(
        "validate-prices", help="Validate top-N container prices against Steam Market API"
    )
    vp_parser.add_argument("--top", type=int, default=10, help="Number of top containers to check")

    # validate-top: enqueue JIT validation for top flip candidates
    vt_parser = subparsers.add_parser(
        "validate-top", help="Enqueue on-demand validation for top flip candidates"
    )
    vt_parser.add_argument("--top", type=int, default=3, help="Number of candidates to validate")
    vt_parser.add_argument("--timeout", type=int, default=60, help="Seconds to wait for result")

    # backfill: fetch Steam Market price history
    bf_parser = subparsers.add_parser(
        "backfill", help="Fetch Steam Market price history for all containers"
    )
    bf_parser.add_argument("--force", action="store_true", help="Bypass rate-limit cooldown")
    bf_parser.add_argument(
        "--missing", action="store_true", help="Only backfill containers with no price data"
    )

    # scrape: run Steam Market container scraper now
    subparsers.add_parser("scrape", help="Run Steam Market container scraper now")

    # seed: re-run static data seeder
    subparsers.add_parser("seed", help="Re-run the static data seeder")

    # reset-db: drop and recreate database
    subparsers.add_parser("reset-db", help="Drop and recreate the entire database (DESTRUCTIVE)")

    # cookie: update Steam session cookie
    subparsers.add_parser("cookie", help="Update Steam session cookie")

    # db: database management sub-commands
    db_parser = subparsers.add_parser("db", help="Database management commands")
    db_sub = db_parser.add_subparsers(dest="db_command")
    db_sub.add_parser("prune", help="Run database garbage collector (tasks, events, VACUUM)")

    # events: event calendar sub-commands
    events_parser = subparsers.add_parser("events", help="Event calendar commands")
    events_sub = events_parser.add_subparsers(dest="events_command")
    events_sub.add_parser("refresh", help="Download and validate updated event calendar YAML")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "start":
        from cli.service import cmd_start
        cmd_start(args)
    elif args.command == "worker":
        from cli.service import cmd_worker
        cmd_worker(args)
    elif args.command == "status":
        from cli.diagnostics import cmd_status
        cmd_status(args)
    elif args.command == "monitor":
        from cli.diagnostics import cmd_monitor
        cmd_monitor(args)
    elif args.command == "watchdog":
        from cli.diagnostics import cmd_watchdog
        cmd_watchdog(args)
    elif args.command == "validate-prices":
        from cli.diagnostics import cmd_validate_prices
        cmd_validate_prices(args)
    elif args.command == "validate-top":
        from cli.diagnostics import cmd_validate_top
        cmd_validate_top(args)
    elif args.command == "backfill":
        from cli.database import cmd_backfill
        cmd_backfill(args)
    elif args.command == "scrape":
        from cli.database import cmd_scrape
        cmd_scrape(args)
    elif args.command == "seed":
        from cli.database import cmd_db_seed
        cmd_db_seed(args)
    elif args.command == "reset-db":
        from cli.database import cmd_db_reset
        cmd_db_reset(args)
    elif args.command == "cookie":
        from cli.setup import cmd_cookie
        cmd_cookie(args)
    elif args.command == "db":
        if getattr(args, "db_command", None) == "prune":
            from cli.database import cmd_db_cleanup
            cmd_db_cleanup(args)
        else:
            print("Usage: cli db <prune>")
            sys.exit(1)
    elif args.command == "events":
        if getattr(args, "events_command", None) == "refresh":
            from cli.database import cmd_events_refresh
            cmd_events_refresh(args)
        else:
            print("Usage: cli events <refresh>")
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
