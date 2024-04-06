# Architecture

This document describes the high-level architecture of molly.

## Bird's Eye View

On the highest level, molly connects to a data warehouse and accepts a series of user-defined data quality rules. With those information, molly monitors the warehouse, and ping a messaging services when something goes wrong.

More specifically, Molly contains different components where each component serves a single responsibility:
    - `Connector` connects to a data warehouse and retrieves information from said warehouse.
    - `Feature` handles the parsing of user-defined rules as well as the validation of output against user's expectations.
    - `Coordinator` assembles the workflow and acts as the connection among all components.
    - `Messenger` communicates with messaging services.
    <!-- TODO: maybe remove scheduler -->
    - `Scheduler` schedules the execution of all processes.
