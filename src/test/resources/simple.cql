CREATE TABLE total_word_counts(
  counter_value counter,
  word_name text,
  PRIMARY KEY (word_name)
); 

CREATE TABLE top_ten_words(
  count int,
  word_name text,
  PRIMARY KEY (word_name, count)
) WITH CLUSTERING ORDER BY (count ASC);