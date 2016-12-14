# Direct parquet reader

There are two ways to read parquet directly in java:

- using Group object

- using SimpleRecord object

These examples are designed to solve the next scenario:

1. We have example parquet file

2. This file includes field **counters** of type *map*

3. With both Group and SimpleRecord object we read this field from the parquet file

My Group example just prints map values.

SimpleRecord does additional work, it collects map values to HashMap.

We used SimpleRecord in our commercial project because it seems to me to be more structured API.