"""Worker-side log streaming: line assembly, thread routing, batching."""

from __future__ import annotations

import io
import threading

from worker.logstream import LineBuffer, StdIORouter, TaskLogStreamer


class TestLineBuffer:
    def collect(self):
        lines = []
        return lines, lambda line, is_stderr: lines.append(line)

    def test_assembles_partial_writes_into_lines(self):
        lines, emit = self.collect()
        buf = LineBuffer(emit, is_stderr=False)
        buf.write("hel")
        buf.write("lo\nwor")
        assert lines == ["hello"]
        buf.write("ld\n")
        assert lines == ["hello", "world"]

    def test_one_write_with_many_newlines_emits_many_lines(self):
        lines, emit = self.collect()
        LineBuffer(emit, is_stderr=False).write("a\nb\nc\n")
        assert lines == ["a", "b", "c"]

    def test_flush_partial_emits_the_unterminated_tail(self):
        lines, emit = self.collect()
        buf = LineBuffer(emit, is_stderr=False)
        buf.write("no newline")
        assert lines == []
        buf.flush_partial()
        assert lines == ["no newline"]
        buf.flush_partial()  # nothing left; must not re-emit
        assert lines == ["no newline"]


class TestStdIORouter:
    def test_always_tees_to_the_fallback(self):
        fallback = io.StringIO()
        router = StdIORouter(fallback, is_stderr=False)
        captured = []
        router.set_sink(lambda text, is_stderr: captured.append(text))
        router.write("both\n")
        assert fallback.getvalue() == "both\n"
        assert captured == ["both\n"]

    def test_no_sink_means_fallback_only(self):
        fallback = io.StringIO()
        router = StdIORouter(fallback, is_stderr=False)
        router.write("plain\n")
        assert fallback.getvalue() == "plain\n"

    def test_sinks_are_thread_local(self):
        router = StdIORouter(io.StringIO(), is_stderr=False)
        seen_a, seen_b = [], []
        barrier = threading.Barrier(2)

        def run(tag, sink_list):
            router.set_sink(lambda text, is_stderr: sink_list.append(text))
            barrier.wait()
            for _ in range(50):
                router.write(f"{tag}\n")
            router.clear_sink()

        threads = [
            threading.Thread(target=run, args=("a", seen_a)),
            threading.Thread(target=run, args=("b", seen_b)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert seen_a == ["a\n"] * 50
        assert seen_b == ["b\n"] * 50


class FakeStub:
    """Records the chunks the streamer ships; consumed on the streamer thread."""

    def __init__(self):
        self.chunks = []
        self.calls = 0

    def StreamTaskLogs(self, chunk_iter):  # noqa: N802 — gRPC stub casing
        self.calls += 1
        self.chunks.extend(chunk_iter)


class TestTaskLogStreamer:
    def test_lines_are_shipped_in_order_with_stream_flags(self):
        stub = FakeStub()
        s = TaskLogStreamer(stub, "job-1", "w-1")
        s.sink("out 1\nout 2\n", is_stderr=False)
        s.sink("err 1\n", is_stderr=True)
        s.close()

        lines = [(ln.line, ln.is_stderr) for c in stub.chunks for ln in c.lines]
        assert lines == [("out 1", False), ("out 2", False), ("err 1", True)]
        assert all(c.job_id == "job-1" and c.worker_id == "w-1" for c in stub.chunks)

    def test_silent_task_never_opens_the_stream(self):
        stub = FakeStub()
        TaskLogStreamer(stub, "job-1", "w-1").close()
        assert stub.calls == 0

    def test_close_flushes_a_partial_line(self):
        stub = FakeStub()
        s = TaskLogStreamer(stub, "job-1", "w-1")
        s.sink("no trailing newline", is_stderr=False)
        s.close()
        lines = [ln.line for c in stub.chunks for ln in c.lines]
        assert lines == ["no trailing newline"]

    def test_interleaved_partial_writes_assemble_per_stream(self):
        stub = FakeStub()
        s = TaskLogStreamer(stub, "job-1", "w-1")
        s.sink("a", is_stderr=False)
        s.sink("x", is_stderr=True)
        s.sink("b\n", is_stderr=False)
        s.sink("y\n", is_stderr=True)
        s.close()
        lines = [(ln.line, ln.is_stderr) for c in stub.chunks for ln in c.lines]
        assert ("ab", False) in lines and ("xy", True) in lines
