#!/usr/bin/env python3
import json
import os
import resource
import subprocess
import sys
import time


def rss_to_mb(value: float) -> float:
    if sys.platform == "darwin":
        return value / (1024 * 1024)
    return value / 1024


def main() -> None:
    if "--" not in sys.argv:
        raise SystemExit("usage: time_cmd.py -- <command> [args...]")
    idx = sys.argv.index("--")
    cmd = sys.argv[idx + 1 :]
    if not cmd:
        raise SystemExit("no command provided")

    start_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    start_time = time.perf_counter()
    proc = subprocess.run(cmd, check=False, stdout=sys.stderr, stderr=sys.stderr)
    end_time = time.perf_counter()
    end_usage = resource.getrusage(resource.RUSAGE_CHILDREN)

    wall_time_s = end_time - start_time
    max_rss = end_usage.ru_maxrss
    if max_rss == 0:
        max_rss = start_usage.ru_maxrss
    peak_rss_mb = rss_to_mb(max_rss) if max_rss else None

    payload = {
        "wall_time_s": wall_time_s,
        "peak_rss_mb": peak_rss_mb,
        "exit_code": proc.returncode,
    }
    print(json.dumps(payload))
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
