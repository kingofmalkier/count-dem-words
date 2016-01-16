CREATE TABLE total_word_counts
  (counter_value counter,
  word_name text,
  PRIMARY KEY (word_name)
);

UPDATE total_word_counts
 SET counter_value = counter_value + 2
 WHERE word_name='this';
