# Architecture

This document describes the high-level architecture of molly.

## Bird's Eye View

On the highest level, molly connects to a data warehouse and accepts a series of user-defined data quality rules. With those information, molly monitors the warehouse continuously, and ping a messaging services when something goes wrong.

More specifically, `Connector` connects to a data warehouse and fetches data. Molly then parses user-defined data quality checks into internal `Feature` objects. `Scheduler` schedules the execution of `Feature` objects and communicates with `Messenger` when necessary. Finally, `Messenger` pings a messaging service when something goes wrong.


### Testing

<!-- TODO -->
