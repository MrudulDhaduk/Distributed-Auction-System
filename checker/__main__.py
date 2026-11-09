"""python -m checker <run_dir>: check a recorded run, print PASS/FAIL per
property, exit 0 only if every property passed."""

import argparse
import sys

from checker.properties import check_all, format_report
from checker.run_files import load_run


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Check a chaos run's history and final dumps.")
    p.add_argument("run_dir", help="directory with history.jsonl, dumps.json, leaders.json")
    args = p.parse_args(argv)
    report = check_all(*load_run(args.run_dir))
    print(format_report(report))
    return 0 if all(r.passed for r in report) else 1


if __name__ == "__main__":
    sys.exit(main())
