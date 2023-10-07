# Architecture

This document describes the high-level architecture of molly.

## Bird's Eye View

<!-- TODO: Insert UML diagram here -->
![]()

On the highest level, molly connects to a data warehouse and accepts a series of user-defined data quality checks. With those information, molly monitors the warehouse continuously, and ping a messaging services when something goes wrong.

More specifically, `Connector` connects to a data warehouse and fetches data. `Indexer` interprets user-defined data quality checks and parses into internally realized `Feature` objects. `Scheduler` schedules the execution of `Feature` objects and communicates with `Messenger` when necessary. Finally, `Messenger` pings a messaging service when something goes wrong.

## Code Map

This section talks briefly about various important directories and data structures.

<!-- TODO -->

### Testing

<!-- TODO -->
