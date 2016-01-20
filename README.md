# count-dem-words

Assumptions made that will be reflected in the implementation:
* WordCount was implemented to meet the API and all "driving" is done via the unit tests. I did not implement any sort of application or front-end around it.
* The difference between "Into" and "into" is irrelevant, and thus all words are lower-cased.
* When updating the current Top Ten, a "contender" must surpass the count of an existing top-ten word. Ties go to the incumbent.
* No jazzy character encodings
* Trailing punctuation on a word is ignored, so "ejected!" counts as "ejected".
* "Words" consisting entirely of punctuation are not counted.
* Words that contain punctuation are treated as interesting words. "Blue-footed" is not broken up into "blue" and "footed".
* Reading any particular top ten should be as close to instantaneous as possible. Processing lines should be the time-intensive operation.
* Any failures in WordCount should be essentially hidden from its caller. Failures should be logged and empty return values returned.
* The code is generally quite optimistic regarding Cassandra's availability and the success of its various queries. A lot more error-checking and logging could be done if so desired.

Testing notes:
* Currently each test in WordCountTest runs with a freshly cleaned Cassandra, this could be changed to speed up tests that are never designed to make actual calls to Cassandra.

To run the tests run the following from the top level of the repo:
./gradlew test

This should automatically download gradle and run the tests with it. You should just need JAVA_HOME set to a valid java installation.
