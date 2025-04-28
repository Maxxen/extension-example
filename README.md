# Example Extension for DuckDB

This is an example extension showcasing how to define aggregate functions through the DuckDB c++ api.
Main code is in [src/example_extension.cpp](src/example_extension.cpp).

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html).

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or
```bash
make test_debug
```
or, after building with `make debug` (or `test_debug`):
```bash
./build/debug/test/unittest --test-dir . test/sql/example.test 
```