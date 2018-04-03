# edgar_sessionizer package

Contents:

## main.py

Contains scripts that run the Sessionation class with different input and output combinations. Functions make instances
of a DataSource object, the Sessionization object, and a Sink object. Any combination of source and sink are allowed
within their own method.

## sessionization.py
Contains the main algorithm for tracking classes.

## sources.py
Contains data input source classes (DataSources).

DataSources have a public `get_next()` method that returns a `RecordRequest` data object.

## sinks.py
Contains output classes for saving or transmission of session data to another

Sinks have a `write()` method that accepts a `Session` object.
