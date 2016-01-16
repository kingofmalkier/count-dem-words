# count-dem-words

Assumptions:
* We don't care about capitilization of the words
* Loading datasets for unit tests could make the test more isolated, but makes tests more fragile regarding changes to data format
*

Design to implement:
* Track word counts by title and across titles separately
** Slightly more work to count words
** Removes need to perform potentially very large operations for any topTenWords call
** Appears to be the nicer thing to do to Cassandra

Example 3 from here: http://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling seems very relevant.
If we replace the joined date with count and do the same ORDER BY (blah DESC) then we can query with a LIMIT of ten to easily get our top ten.

oh ho! Counters! https://docs.datastax.com/en/cql/3.1/cql/cql_using/use_counter_t.html