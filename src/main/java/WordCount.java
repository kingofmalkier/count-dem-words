import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import javax.print.DocFlavor;
import java.util.HashMap;
import java.util.Map;



/** A service that counts words in books. */
public class WordCount {
    private Session session;

    /**
     * Count all words in a given book in preparation for later querying.
     *
     * @param bookTitle The title of the book. Must be a non-empty String.
     * @param lines     The book's text. Must not be null.
     */
    public void countWords(String bookTitle, Iterable<String> lines) {
        if (bookTitle == null || bookTitle.isEmpty()) {
            throw new IllegalArgumentException("The book's title must be a non-empty String.");
        }

        if (lines == null) {
            throw new IllegalArgumentException("Lines must not be null.");
        }

        for (String line : lines) {
            countWordsInLine(bookTitle, line.toLowerCase());
        }
    }

    private void countWordsInLine(String bookTitle, String line) {
        for (String word : line.split(" ")) {
            //TODO: Also need to increment the title-specific values
            //TODO: Strip off punctuation at front or end of word? 'cliff!' should count as 'cliff', yes?
            long currentCount = getCurrentCountForWord(word);
            incrementCountForWord(word);
            updateTopTen(word, currentCount + 1L);
        }
    }

    private long getCurrentCountForWord(String word) {
        ResultSet results = getSession().execute("SELECT * FROM total_word_counts WHERE word_name='" + word + "';");

        for (Row row : results) {
            return row.getLong("counter_value");
        }

        return 0;
    }

    private void incrementCountForWord(String word) {
        getSession().execute("UPDATE total_word_counts\n" +
                " SET counter_value = counter_value + 1\n" +
                " WHERE word_name='" + word + "';"); //TODO: Safe to hardcode 10 when dealing with topTen()?
        //TODO: Why does that have LIMIT?
    }

    private void updateTopTen(String word, long currentCount) {
        ResultSet results = getSession().execute("SELECT COUNT(*) FROM top_ten_words;");
        int numRowsInTopTen = 0;
        for (Row row : results) {
            numRowsInTopTen = (int)row.getLong(0);
        }

        if (numRowsInTopTen < 10) {
            /*
             *TODO: This is doing replaces even when the word is already present.
             * Most importantly, it's doing the delete part, so we usually end up with
             * multiple rows of one word, and some get deleted. When we collect those into
             * a hash map the duplicates overwrite each other and that's why 7 instead of 10
             * Need to be more clever about getting the entire top ten and passing through it once,
             * retaining the lowest word's name and also checking for ourselves.
             * If we find ourselves we need to delete ourselves and readd with a higher number
             */
            replaceTopTen(null, word, currentCount);
            return;
        }

        results = getSession().execute("SELECT * FROM top_ten_words WHERE count > " + currentCount + " ALLOW FILTERING;");

        for (Row row : results) {
            if (currentCount > row.getInt("count")) {
                replaceTopTen(row.getString("word_name"), word, currentCount);
            }
        }
    }

    private void replaceTopTen(String oldWord, String newWord, long count) {
        if (oldWord != null) {
            getSession().execute("DELETE FROM top_ten_words where word_name = '" + oldWord + "';");
        }

        getSession().execute("INSERT INTO top_ten_words (word_name, count)\n" +
                " VALUES ('" + newWord + "', " + count + ");");
    }

    /**
     * Provides the top ten most common words found cumulatively across all
     * books that have been processed by the service.
     *
     * @return A mapping of at most ten words to their respective counts. Never
     *         null, but possibly empty.
     */
    public Map<String, Integer> topTenWords() {
        Map<String, Integer> topTenMap = new HashMap<>();

        //TODO: Hide the session detail at a minimum
        ResultSet results = getSession().execute("SELECT * FROM top_ten_words");

        for (Row row : results) {
            String word = row.getString("word_name");
            Integer count = row.getInt("count");

            topTenMap.put(word, count);
        }

        return topTenMap;
    }

    /**
     * Provides the top ten most common words found in a book that was already
     * processed by the service.
     *
     * @param bookTitle The title of the book. Must be a non-empty String.
     * @return A mapping of at most ten words to their respective counts. Never
     *         null, but possibly empty.
     */
    public Map<String, Integer> topTenWords(String bookTitle) {
        if (bookTitle == null || bookTitle.isEmpty()) {
            throw new IllegalArgumentException("The book's title must be a non-empty String.");
        }

        return new HashMap<>();
    }

    private Session getSession() {
        if (session == null) {
            session = connect("127.0.0.1");
        }

        return session;
    }

    private Session connect(String node) {
        return Cluster.builder()
                .addContactPoint(node).withPort(9142)
                .build().connect("wordsKS"); //TODO: Hardcoded?
    }
}
