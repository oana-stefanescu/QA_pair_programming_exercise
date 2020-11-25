# partitioning-service
This service is used to create Impala / Hive queries for a given time range.
That is it can be used to create a query string to be used in the `WHERE` clause of a query to make use of the
partitions for year, month, day and hour.

# Dates and Times
A datetime is always passed as a string in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601).
If no UTC offset / timezone is present the date is assumed to be in UTC:

* 2020-11-25T15:00:00+01:00 is 25th November 2020, hour 14 UTC
* 2020-11-25T15:00:00 is 25th November 2020, hour 15 UTC

## Impala and Hive
A result for the Impala and Hive endpoint could look like this:

```sql
`timestamp` BETWEEN 1606230000 AND 1606327199 AND ((`year` = 2020 AND `month` = 11 AND `day` = 24 AND `hour` BETWEEN 15 AND 23) OR (`year` = 2020 AND `month` = 11 AND `day` = 25 AND `hour` BETWEEN 0 AND 17))
```

Both also have an option to skip the `timestamp` part (if you only want to look at the file directly for example).
Then the result would look like this:

```sql
((`year` = 2020 AND `month` = 11 AND `day` = 24 AND `hour` BETWEEN 15 AND 23) OR (`year` = 2020 AND `month` = 11 AND `day` = 25 AND `hour` BETWEEN 0 AND 17))
``` 
