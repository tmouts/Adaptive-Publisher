import os
import time
from collections import deque
from adaptive_publisher.event_publishers.publisher import EventPublisher

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return default if v is None or v == "" else int(v)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return default if v is None or v == "" else float(v)


def _set_span_tag(span, key: str, value):
    
    if span is None:
        return
    if hasattr(span, "set_tag"):
        span.set_tag(key, value)
    elif hasattr(span, "set_attribute"):
        span.set_attribute(key, value)


class _AdaptiveBatcher:

    def __init__(self):
        self.strategy = os.getenv("MICRO_BATCH_STRATEGY", "adaptive").lower()
        self.fixed_size = max(1, _env_int("MICRO_BATCH_FIXED_SIZE", 3))
        self.min_size = max(1, _env_int("MICRO_BATCH_MIN_SIZE", 1))
        self.max_size = max(self.min_size, _env_int("MICRO_BATCH_MAX_SIZE", 8))
        self.max_wait_ms = max(0, _env_int("MICRO_BATCH_MAX_WAIT_MS", 50))

        self.target_send_ms = max(0.1, _env_float("MICRO_BATCH_TARGET_SEND_MS", 18.0))
        self.ewma_alpha = min(max(_env_float("MICRO_BATCH_EWMA_ALPHA", 0.2), 0.01), 1.0)

        self._buf = deque() 
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mb = _AdaptiveBatcher()

    def generate_event_from_frame(self, *args, **kwargs):
        return super().generate_event_from_frame(*args, **kwargs)

    def generate_and_send_event(self, frame, *args, **kwargs):
       
        with self.tracer.start_active_span('generate_and_send_event', child_of=span_ctx) as scope:
            event_data = self.generate_event_from_frame(frame, frame_index)
        span_ctx = self.tracer.extract(Format.HTTP_HEADERS, {'uber-trace-id': trace_id})

        event = self.generate_event_from_frame(frame, *args, **kwargs)

        batch = self._mb.add(event)

        _set_span_tag(span, "microbatch.strategy", self._mb.strategy)
        _set_span_tag(span, "microbatch.batch_size", self._mb.batch_size)

        if batch:
            t0 = time.perf_counter()

            pipe = self.redis_client.pipeline(transaction=False)

            for ev in batch:
                self._send_one_event_exactly_like_original(ev, pipe=pipe)

            pipe.execute()

            send_ms = (time.perf_counter() - t0) * 1000.0
            self._mb.report_send_ms(send_ms)

            _set_span_tag(span, "microbatch.flush_n", len(batch))
            _set_span_tag(span, "microbatch.redis_send_ms", round(send_ms, 2))
            _set_span_tag(span, "microbatch.next_batch_size", self._mb.batch_size)

        return event

    def _send_one_event_exactly_like_original(self, event, *, pipe):
        
       pipe = self.bufferstream.pipeline(transaction=False)

    for event_data in batch:
     self.parent_service.write_event_with_trace(event_data,pipe)
            self.logger.info(f'sending event_data "{event_data}", to buffer stream: "{self.buffer_stream_key}"')

pipe.execute()


raise NotImplementedError(self.parent_service.write_event_with_trace(event_data,pipe))
