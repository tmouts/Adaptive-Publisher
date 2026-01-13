import os
import time
from collections import deque

# Import the original EventPublisher
from adaptive_publisher.event_publishers.publisher import EventPublisher


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return default if v is None or v == "" else int(v)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return default if v is None or v == "" else float(v)


def _set_span_tag(span, key: str, value):
    """Works with either OpenTracing spans (set_tag) or OpenTelemetry spans (set_attribute)."""
    if span is None:
        return
    if hasattr(span, "set_tag"):
        span.set_tag(key, value)
    elif hasattr(span, "set_attribute"):
        span.set_attribute(key, value)


class _AdaptiveBatcher:
    """
    Adaptive micro-batching:
      - flush when buffer reaches batch_size OR max_wait_ms exceeded
      - batch_size updated online based on observed redis send time (EWMA + gentle step)
    """

    def __init__(self):
        self.strategy = os.getenv("MICRO_BATCH_STRATEGY", "adaptive").lower()  # none|fixed|adaptive
        self.fixed_size = max(1, _env_int("MICRO_BATCH_FIXED_SIZE", 3))
        self.min_size = max(1, _env_int("MICRO_BATCH_MIN_SIZE", 1))
        self.max_size = max(self.min_size, _env_int("MICRO_BATCH_MAX_SIZE", 8))
        self.max_wait_ms = max(0, _env_int("MICRO_BATCH_MAX_WAIT_MS", 50))

        self.target_send_ms = max(0.1, _env_float("MICRO_BATCH_TARGET_SEND_MS", 18.0))
        self.ewma_alpha = min(max(_env_float("MICRO_BATCH_EWMA_ALPHA", 0.2), 0.01), 1.0)

        self._buf = deque()  # items are (event_dict, created_ts)
        self._batch_size = self.fixed_size
        self._send_ms_ewma = None

    @property
    def batch_size(self):
        if self.strategy == "none":
            return 1
        if self.strategy == "fixed":
            return self.fixed_size
        return self._batch_size

    def add(self, event):
        if self.strategy == "none":
            return [event]

        self._buf.append((event, time.perf_counter()))

        # flush by size
        if len(self._buf) >= self.batch_size:
            return self._drain(self.batch_size)

        # flush by time
        if self.max_wait_ms > 0 and self._buf:
            oldest_ts = self._buf[0][1]
            waited_ms = (time.perf_counter() - oldest_ts) * 1000.0
            if waited_ms >= self.max_wait_ms:
                return self._drain(len(self._buf))

        return None

    def flush(self):
        if not self._buf:
            return None
        return self._drain(len(self._buf))

    def report_send_ms(self, send_ms: float):
        if self.strategy != "adaptive":
            return
        x = float(send_ms)
        self._send_ms_ewma = x if self._send_ms_ewma is None else (
            self.ewma_alpha * x + (1.0 - self.ewma_alpha) * self._send_ms_ewma
        )

        ratio = self.target_send_ms / max(1e-6, self._send_ms_ewma)
        proposed = int(round(self._batch_size * ratio))

        # gentle step (avoid oscillations)
        if proposed > self._batch_size:
            proposed = self._batch_size + 1
        elif proposed < self._batch_size:
            proposed = self._batch_size - 1

        self._batch_size = max(self.min_size, min(self.max_size, proposed))

    def _drain(self, n: int):
        out = []
        for _ in range(n):
            out.append(self._buf.popleft()[0])
        return out


class AdaptiveMicroBatchEventPublisher(EventPublisher):
    """
    Drop-in replacement for EventPublisher that:
      - leaves event structure intact (1 event per frame)
      - sends events using micro-batching via Redis pipeline
      - adds Jaeger tags for comparison screenshots
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mb = _AdaptiveBatcher()

    # --- Compression lives here (your teammates will implement variants) ---
    # For YOUR micro-batching implementation, simplest is to reuse the base one:
    def generate_event_from_frame(self, *args, **kwargs):
        return super().generate_event_from_frame(*args, **kwargs)

    # --- Micro-batching lives here (YOUR part) ---
    def generate_and_send_event(self, frame, *args, **kwargs):
        """
        Supervisor request:
          - KEEP the event tracing lines from the original (lines 87-88)
          - KEEP the actual transmission line from the original (line 91)

        Because I can't see your exact local line-87/88/91, do this:
          1) open the original publisher.py in your repo
          2) copy the two tracing lines (87-88) into the marked section below
          3) copy the transmission line (91) into _send_one_event_exactly_like_original()
        """

        # ---------------------------
        # (A) KEEP LINES 87-88 HERE (COPY/PASTE EXACTLY)
        # Example patterns you might have:
        #   with self.tracer.start_active_span("process_next_frame") as scope:
        #       span = scope.span
        #
        # Put your real two lines here and ensure you still end up with a `span` variable.
        # ---------------------------
        span = None  # <-- REPLACE by your copied tracing lines

        # Build event for this frame (uses your compression path)
        event = self.generate_event_from_frame(frame, *args, **kwargs)

        batch = self._mb.add(event)

        _set_span_tag(span, "microbatch.strategy", self._mb.strategy)
        _set_span_tag(span, "microbatch.batch_size", self._mb.batch_size)

        if batch:
            t0 = time.perf_counter()

            # Redis pipeline to send multiple events "together"
            pipe = self.redis_client.pipeline(transaction=False)

            for ev in batch:
                # keep the exact original transmission line (line 91) inside this helper
                self._send_one_event_exactly_like_original(ev, pipe=pipe)

            pipe.execute()

            send_ms = (time.perf_counter() - t0) * 1000.0
            self._mb.report_send_ms(send_ms)

            _set_span_tag(span, "microbatch.flush_n", len(batch))
            _set_span_tag(span, "microbatch.redis_send_ms", round(send_ms, 2))
            _set_span_tag(span, "microbatch.next_batch_size", self._mb.batch_size)

        return event

    def _send_one_event_exactly_like_original(self, event, *, pipe):
        """
        COPY your original publisher.py line 91 into this function.
        Only change the redis client object name to `pipe` (so it pipelines).
        Nothing else.

        Example (NOT your real line):
            pipe.xadd(self.service_stream_key, event)

        Put your repo’s real transmission line here.
        """
        # ---------------------------
        # (B) KEEP LINE 91 HERE (COPY/PASTE EXACTLY, but use `pipe` instead of direct client)
        # ---------------------------
        raise NotImplementedError("Paste original transmission line here using `pipe`.")
