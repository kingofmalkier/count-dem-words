# count-dem-words

Assumptions:
* We don't care about capitilization of the words
* A "contender" must surpass the count of an existing top-ten word to make the top-ten
* Loading datasets for unit tests could make the test more isolated, but makes tests more fragile regarding changes to data format
* Currently each test in WordCountTest runs with Cassandra, this could be changed for speed but for ease-of-code-review's sake I will leave it

Design to implement:
* Track word counts by title and across titles separately
** Slightly more work to count words
** Removes need to perform potentially very large operations for any topTenWords call
** Appears to be the nicer thing to do to Cassandra

Example 3 from here: http://www.datastax.com/dev/blog/basic-rules-of-cassandra-data-modeling seems very relevant.
If we replace the joined date with count and do the same ORDER BY (blah DESC) then we can query with a LIMIT of ten to easily get our top ten.

oh ho! Counters! https://docs.datastax.com/en/cql/3.1/cql/cql_using/use_counter_t.html

if we have a table:

pos|word|count

We can have position be the primary key so that we can actually mutate the word and count (necessary? update may be able to change all values). Then whenever we update a word's count we also read the count and consider it good enough for ranking. (The increment function should ensure that we keep the correct total, even if our ranking isn't always using the most up-to-date value.) Then we go through the current top-ten table. If we find a row where our current count is bigger, we update that row to have our word and count values. When we want the top ten we just grab that table. The main con here is that we do a lot of reads just to write one word. The main pro is that when we want our top ten it's super-duper fast. This may be acceptable since presumably getting the top ten is the single most time sensitive operation. Hell, in many instances you could probably cache the top ten results for at least a minute or something and therefore basically the only reads are the ones done while counting/processing.

If there are less than 10 numbers, we are either adding a new row or replacing ourselves
If there are 10 numbers, we are either replacing the lowest or replacing ourselves

When determining the replacement we only need to see numbers smaller than our replacement count; this will even find ourselves if relevant.