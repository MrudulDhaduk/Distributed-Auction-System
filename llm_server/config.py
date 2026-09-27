"""Constants for the LLM server: model, timeout, port, output caps.

Here rather than inline in llm_servicer.py so the three RPC handlers cannot
drift apart -- one model name, one timeout, one place to change them. The
same reasoning as common/wire.py: a value shared by more than one caller
belongs somewhere both can read it.
"""

# The instruct-tuned variant, not the hybrid qwen3:4b. The hybrid emits
# <think> blocks that could not be reliably suppressed and that dominated
# CPU latency; see docs/design-notes.md for the measurements that settled
# it.
MODEL = "qwen3:4b-instruct-2507-q4_K_M"

# A backstop against a hung or dead daemon, not a latency target. The
# measured spread is 3.8-5.5s, so this should only ever fire when something
# is actually wrong -- never on a merely slow call. See docs/design-notes.md.
TIMEOUT_SECONDS = 30.0

# 50051 is the auction server (server/serve.py). The LLM server is a
# separate process on a separate node, so it needs its own port.
PORT = 50052

# Output caps, in tokens, passed to Ollama as `num_predict`.
#
# These are a LATENCY control, not a style preference. Generation on CPU is
# roughly linear in tokens produced, so an uncapped model writing six
# paragraphs is the difference between a 4s call and a 40s one -- and this
# call sits on the request path of a bidder waiting for an answer. The
# prompts ask for brevity as well; the cap is what enforces it when the
# model ignores the ask.
#
# The cap is a HARD stop, not a hint -- generation simply ends, mid-word if
# that is where the count runs out. So a cap set too close to the length the
# prompt asks for yields truncated prose rather than short prose. The summary
# cap was raised from 150 after a run that enumerated four bids with full
# timestamps and was cut off mid-sentence; the prompt now asks for counts
# instead of a list, and the headroom covers the case where it enumerates
# anyway.
MAX_TOKENS_FAQ = 80           # "a sentence or two"
MAX_TOKENS_DESCRIPTION = 130  # a short paragraph
MAX_TOKENS_SUMMARY = 170      # a short paragraph, plus room for numbers
