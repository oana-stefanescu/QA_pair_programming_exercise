# partitioning-service
This service is used to create Impala / Hive queries for a given time range.
That is it can be used to create a query string to be used in the `WHERE` clause of a query to make use of the
partitions for year, month, day and hour.

## Impala and Hive
A result for the Impala and Hive endpoint could look like this:

```sql
`year` = 2020 AND `month` = 11 AND `day` BETWEEN 1 AND 13
```
