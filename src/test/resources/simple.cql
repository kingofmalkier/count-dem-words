CREATE TABLE total_word_counts(
  counter_value counter,
  word_name text,
  PRIMARY KEY (word_name)
); 

CREATE TABLE top_ten_words(
  title text,
  PRIMARY KEY (title)
);

ALTER TABLE top_ten_words ADD counts map<text, int>