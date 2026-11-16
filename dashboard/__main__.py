"""python -m dashboard [--llm real|fake|none] [--port 8765] [--run-dir DIR] [--open]"""

import argparse
import datetime
import logging
import os
import threading
import webbrowser

from dashboard.server import Dashboard, serve
from harness.procs import ROOT


def main(argv=None):
    p = argparse.ArgumentParser(description="Start the cluster and the web dashboard.")
    p.add_argument("--llm", choices=("real", "fake", "none"), default="real",
                   help="real: llm_server with Ollama (default); fake: canned answers; none: no LLM server")
    p.add_argument("--port", type=int, default=8765, help="dashboard HTTP port (default 8765)")
    p.add_argument("--run-dir", default=None,
                   help="where node data and logs go (default: a new runs\\dashboard-<time>). "
                        "Pass an existing one to restart on the same logs.")
    p.add_argument("--open", action="store_true", help="open the page in the default browser")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("client.auction_client").setLevel(logging.WARNING)
    run_dir = args.run_dir or os.path.join(
        ROOT, "runs", "dashboard-" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

    dash = Dashboard(run_dir, llm=args.llm)
    try:
        httpd = serve(dash, port=args.port)  # bind first: fail fast if the port is taken
    except OSError as e:
        raise SystemExit(f"error: cannot listen on 127.0.0.1:{args.port} ({e}); try --port <another>")
    dash.start()
    url = f"http://127.0.0.1:{args.port}/"
    print(f"\n  dashboard: {url}\n  run dir:   {run_dir}\n  node logs: {os.path.join(run_dir, 'logs')}\n"
          "  Ctrl+C stops the dashboard AND the cluster it started.\n", flush=True)
    if args.open:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    try:
        httpd.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        dash.stop()


if __name__ == "__main__":
    main()
