# CLAUDE.md

## What is this?

StratQueue is a .NET 9 NuGet package: an in-memory work queue with pluggable dequeue strategies (FIFO, round-robin) and SQLite persistence. See `ROADMAP.md` for full design.

## Build & Test

```bash
dotnet build StratQueue.sln
dotnet test StratQueue.sln
```

## Project Structure

- `src/StratQueue.Core/` — the library (namespace: `StratQueue`)
- `tests/StratQueue.Tests/` — xUnit tests

## Conventions

- All SQL lives in `Internal/SqliteJournal.cs` — no ORM, parameterized queries only
- `Internal/` classes are `internal` — exposed to tests via `InternalsVisibleTo`
- Strategy logic is pure C# (in-memory), SQLite is only for persistence/recovery
- All public types use `record` where appropriate
