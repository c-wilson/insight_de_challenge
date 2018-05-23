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

DataSources have a public `get_next()` method that returns a dictionary containing at minimum:
    
    1. "ip": string representing the client's ip for the request,
    1. "timestamp": float representing time in seconds since UNIX epoch.

## sinks.py
Contains output classes for saving or transmission of session data to another

Sinks have a `write()` method that accepts a `Session` object.
