"""Throwaway smoke test: can we reach the local Ollama model from Python?

Task 4, step 1. No gRPC, no auction code, no Raft. The only questions this
answers are "does a prompt go out and does text come back", and "how slow and
how consistent is it?"

Model is qwen3:4b-instruct-2507, the instruct-tuned variant. It never emits
<think> blocks, so the call is a plain ollama.chat with no thinking flag and
no prompt switches. See docs/design-notes.md for why the hybrid qwen3:4b was
dropped.
"""

import statistics
import time

from ollama import Client

MODEL = "qwen3:4b-instruct-2507-q4_K_M"
PROMPT = "Explain what an auction reserve price is in two sentences"
RUNS = 6

# Generous relative to the measured spread -- see docs/design-notes.md. This
# is a backstop against a hung daemon, not a latency target: it should only
# ever fire when something is actually wrong.
TIMEOUT_SECONDS = 30.0

# A clean answer starts answering. These are the openers a reasoning model
# leaks when its <think> block ends up inline in the response.
THINK_TAGS = ("<think>", "</think>")
REASONING_OPENERS = ("okay", "alright", "hmm", "the user", "let me", "we are")

# The Client wrapper exists only so we can set a timeout; it is passed
# straight through to the underlying httpx client, which the module-level
# ollama.chat() helper gives no way to configure.
client = Client(timeout=TIMEOUT_SECONDS)


def is_clean(answer: str) -> bool:
    """True if the answer carries no reasoning tags or reasoning preamble."""
    if any(tag in answer for tag in THINK_TAGS):
        return False
    return not answer.strip().lower().startswith(REASONING_OPENERS)


def ask() -> tuple[str, float]:
    """Send PROMPT once. Returns (answer, wall_clock_seconds)."""
    start = time.perf_counter()
    response = client.chat(
        model=MODEL,
        messages=[{"role": "user", "content": PROMPT}],
    )
    return response.message.content, time.perf_counter() - start


def main() -> None:
    print(f"model   : {MODEL}")
    print(f"prompt  : {PROMPT}")
    print(f"timeout : {TIMEOUT_SECONDS}s")

    # The first call pays a one-time cost to load 2.5 GB of weights into
    # memory. Measuring it would skew the first sample, so burn it here.
    print("warming up ...")
    client.chat(model=MODEL, messages=[{"role": "user", "content": "hi"}])

    times: list[float] = []
    dirty: list[str] = []
    for i in range(1, RUNS + 1):
        answer, elapsed = ask()
        times.append(elapsed)
        clean = is_clean(answer)
        if not clean:
            dirty.append(answer)
        print(f"run {i}: {elapsed:6.2f}s  {len(answer):4d} chars  "
              f"clean={'yes' if clean else 'NO'}")

    print(f"\nmin {min(times):.2f}s | median {statistics.median(times):.2f}s "
          f"| max {max(times):.2f}s")
    print(f"clean: {RUNS - len(dirty)}/{RUNS} runs")
    for bad in dirty:
        print(f"\n!! not clean:\n{bad}")

    print(f"\nlast answer: {answer.strip()}")


if __name__ == "__main__":
    main()
