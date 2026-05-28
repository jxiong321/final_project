import asyncio
from pathlib import Path
from typing import Annotated

import typer

from pipeline.sources import CSVSource, JSONLSource
from pipeline.sinks import CSVSink, JSONLSink
from pipeline.pipeline import Pipeline

app = typer.Typer(help="Async data pipeline : stream CSV/JSONL sources through transforms into a sink.")


def _make_source(path: str) -> CSVSource | JSONLSource:
    p = Path(path)
    if p.suffix == ".csv":
        return CSVSource(path)
    if p.suffix in (".jsonl", ".ndjson"):
        return JSONLSource(path)
    raise typer.BadParameter(f"Unsupported source format: {p.suffix!r} (use .csv or .jsonl)")


def _make_sink(output: str, headers: list[str]) -> CSVSink | JSONLSink:
    p = Path(output)
    if p.suffix == ".csv":
        return CSVSink(output, headers)
    if p.suffix in (".jsonl", ".ndjson"):
        return JSONLSink(output, headers)
    raise typer.BadParameter(f"Unsupported output format: {p.suffix!r} (use .csv or .jsonl)")


@app.command()
def run(
    sources: Annotated[list[str], typer.Argument(help="Input files (.csv or .jsonl). Pass multiple to merge.")],
    output: Annotated[str, typer.Option("--output", "-o", help="Output file (.csv or .jsonl).")],
    headers: Annotated[list[str], typer.Option("--header", "-H", help="Output column names (repeat for each).")],
    dry: Annotated[bool, typer.Option("--dry", help="Validate transforms against a sample without writing output.")] = False,
) -> None:
    """Run the pipeline over one or more source files and write to an output sink."""

    pipeline = Pipeline(sink=_make_sink(output, headers))
    for src in sources:
        pipeline.add_source(_make_source(src))

    async def _run() -> None:
        if dry:
            report = await pipeline.dry_run()
            pipeline.print_dry_run(report)
        else:
            stats = await pipeline.run()
            stats.print()

    asyncio.run(_run())


@app.command()
def dry_run(
    sources: Annotated[list[str], typer.Argument(help="Input files (.csv or .jsonl).")],
    headers: Annotated[list[str], typer.Option("--header", "-H", help="Output column names (repeat for each).")] = [],
) -> None:
    """Validate sources and transforms without writing any output."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=True) as tmp:
        pipeline = Pipeline(sink=JSONLSink(tmp.name, headers))
        for src in sources:
            pipeline.add_source(_make_source(src))

        async def _run() -> None:
            report = await pipeline.dry_run()
            pipeline.print_dry_run(report)

        asyncio.run(_run())


def main() -> None:
    app()
