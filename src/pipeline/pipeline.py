import asyncio
from dataclasses import dataclass
from typing import Any, Callable
import time

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.text import Text

from pipeline.sources import Source, Record

console = Console()


@dataclass
class TransformMetadata:
    func: Callable[[Record], Record | None]
    input_fields: dict[str, type]
    kind: str
    name: str


@dataclass
class PipelineStats:
    records_ingested_per_source: dict[str, int]
    records_dropped_per_transform: dict[str, int]
    records_written: int
    records_errored_per_transform: dict[str, int]
    walltime: float

    def print(self) -> None:
        """print a nice summary table"""
        console.print()
        console.rule("[bold cyan]Pipeline Run Complete[/bold cyan]")

        src_table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold magenta")
        src_table.add_column("Source", style="cyan")
        src_table.add_column("Records Ingested", justify="right")
        for name, count in self.records_ingested_per_source.items():
            src_table.add_row(name, str(count))
        console.print(src_table)

        summary = Table(box=box.SIMPLE_HEAVY, show_header=False)
        summary.add_column("Metric", style="bold")
        summary.add_column("Value", justify="right")
        summary.add_row("Records written", str(self.records_written))
        summary.add_row("Wall time", f"{self.walltime:.3f}s")
        if self.records_dropped_per_transform:
            dropped = ", ".join(f"{k}: {v}" for k, v in self.records_dropped_per_transform.items())
            summary.add_row("Dropped", dropped)
        if self.records_errored_per_transform:
            errored = ", ".join(f"{k}: {v}" for k, v in self.records_errored_per_transform.items())
            summary.add_row("[red]Errored[/red]", errored)
        console.print(summary)


class Sink:
    """An object for data to go into. any object with write() and close() works as a sink."""
    def write(self, record: Record) -> None: ...
    def close(self) -> None: ...


class Pipeline:
    def __init__(self, sink: Any, error_sink: Any = None) -> None:
        self.sink = sink
        self.error_sink = error_sink
        self.sources: list[Source] = []
        self.transforms: list[TransformMetadata] = []

        self.records_ingested_per_source: dict[str, int] = {}
        self.records_dropped_per_transform: dict[str, int] = {}
        self.records_errored_per_transform: dict[str, int] = {}
        self.records_written: int = 0
        self.walltime: float = 0.0

    def _validate_record(self, record: Record, trans: TransformMetadata) -> str:
        """Return 'ok' if all required fields are present, 'skip' otherwise."""
        for field in trans.input_fields:
            if field not in record:
                return "skip"
        return "ok"

    def add_source(self, source: Source) -> None:
        self.sources.append(source)

    async def run(self) -> PipelineStats:
        queue: asyncio.Queue[Record | None] = asyncio.Queue(maxsize=20)

        start = time.perf_counter()

        producer_task = asyncio.create_task(self._produce(queue))
        consumer_task = asyncio.create_task(self._consume(queue))

        await producer_task
        await consumer_task

        self.walltime = time.perf_counter() - start
        self.sink.close()

        return PipelineStats(
            self.records_ingested_per_source,
            self.records_dropped_per_transform,
            self.records_written,
            self.records_errored_per_transform,
            self.walltime,
        )

    async def _consume(self, queue: asyncio.Queue[Record | None]) -> None:
        while True:
            record = await queue.get()
            if record is None:
                break

            drop = False
            for trans in self.transforms:
                status = self._validate_record(record, trans)

                if status == "skip":
                    continue

                try:
                    record = trans.func(record)
                except Exception as e:
                    self.records_errored_per_transform[trans.name] = \
                        self.records_errored_per_transform.get(trans.name, 0) + 1
                    if self.error_sink:
                        self.error_sink.write({"record": record, "transform": trans.name, "reason": str(e)})
                    drop = True
                    break

                if record is None:
                    self.records_dropped_per_transform[trans.name] = \
                        self.records_dropped_per_transform.get(trans.name, 0) + 1
                    drop = True
                    break

            if drop:
                continue
            self.sink.write(record)
            self.records_written += 1

    async def _produce(self, queue: asyncio.Queue[Record | None]) -> None:
        async with asyncio.TaskGroup() as tg:
            for source in self.sources:
                tg.create_task(source.stream(queue))
        for source in self.sources:
            self.records_ingested_per_source[source.name] = source.records_ingested
        await queue.put(None)

    def transform(
        self,
        input_fields: dict[str, type] | None = None,
        kind: str = "",
    ) -> Callable[[Callable[[Record], Record | None]], Callable[[Record], Record | None]]:
        def decorator(func: Callable[[Record], Record | None]) -> Callable[[Record], Record | None]:
            self.transforms.append(TransformMetadata(
                func=func,
                input_fields=input_fields or {},
                kind=kind,
                name=func.__name__,
            ))
            console.log(f"[dim]Registered transform:[/dim] [bold]{func.__name__}[/bold]  fields={input_fields}  kind={kind!r}")
            return func
        return decorator

    async def _sample(self, source: Source, n: int = 1) -> list[Record]:
        """Pulls up to n records from source. note: drains the whole source, not just n records."""
        q: asyncio.Queue[Record | None] = asyncio.Queue()
        await source.stream(q)
        samples: list[Record] = []
        while not q.empty() and len(samples) < n:
            item = await q.get()
            if item is not None:
                samples.append(item)
        return samples

    async def dry_run(self, sample_size: int = 1) -> list[str]:
        """Validate the transform chain against a sample from each source. no output """
        report: list[str] = []
        for source in self.sources:
            report.append(f"\n{source.name}:")
            samples = await self._sample(source, sample_size)
            if not samples:
                report.append("  (no records found)")
                continue

            for raw in samples:
                record = dict(raw)
                clean = True
                for trans in self.transforms:
                    skip = False
                    for field, expected_type in trans.input_fields.items():
                        if field not in record:
                            report.append(f"  [skip ] {trans.name}: needs '{field}', not present")
                            skip = True
                            clean = False
                            break
                        if not isinstance(record[field], expected_type):
                            got = type(record[field]).__name__
                            report.append(f"  [TYPE ] {trans.name}: '{field}' is {got}, expected {expected_type.__name__}")
                            clean = False
                    if skip:
                        continue

                    try:
                        record = trans.func(record)  # type: ignore[assignment]
                    except Exception as e:
                        report.append(f"  [CRASH] {trans.name}: {type(e).__name__}: {e}")
                        clean = False
                        break
                if clean:
                    report.append("  [OK   ] Sample chain validates clean")
        return report

    def print_dry_run(self, report: list[str]) -> None:
        """Print a dry run report"""
        lines: list[Text] = []
        for line in report:
            t = Text()
            stripped = line.strip()
            if stripped.startswith("[skip ]"):
                t.append("  ⚠ ", style="yellow bold")
                t.append(stripped[7:], style="yellow")
            elif stripped.startswith("[TYPE ]"):
                t.append("  ✗ ", style="red bold")
                t.append(stripped[7:], style="red")
            elif stripped.startswith("[CRASH]"):
                t.append("  ✗ ", style="red bold")
                t.append(stripped[7:], style="red bold")
            elif stripped.startswith("[OK"):
                t.append("  ✓ ", style="green bold")
                t.append(stripped[7:], style="green")
            elif stripped.endswith(":"):
                t.append(f"\n{stripped}", style="bold cyan")
            else:
                t.append(line)
            lines.append(t)

        body = Text("\n").join(lines)
        console.print(Panel(body, title="[bold]Dry Run Report[/bold]", border_style="cyan", box=box.ROUNDED))
