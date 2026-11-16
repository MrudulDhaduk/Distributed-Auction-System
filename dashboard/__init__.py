"""Web dashboard for the auction cluster: `python -m dashboard`.

It starts its own LLM server and three nodes (harness/procs.py LocalCluster),
serves one page on http://127.0.0.1:8765, and talks to the cluster only
through the same interfaces everything else uses:

  StatusService.GetNodeStatus   every node, 10 times a second (roles, terms,
                                indexes, heartbeat counters, election timers)
  StatusService.DumpState       the leader, 3 times a second (auctions, bids,
                                escrow, summaries, registered users)
  ClusterClient                 every write and every LLM question, one
                                client per logged-in browser session

Kill / restart / suspend / resume act on the node processes it started.
"""
