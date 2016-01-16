import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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
            incrementCountForWord(word);
        }
    }

    private void incrementCountForWord(String word) {
        getSession().execute("UPDATE total_word_counts\n" +
                " SET counter_value = counter_value + 1\n" +
                " WHERE word_name='" + word + "';");
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
        ResultSet results = getSession().execute("SELECT * FROM total_word_counts");

        //TODO: Limit to 10 words
        for (Row row : results) {
            String word = row.getString("word_name");
            Integer count = (int)row.getLong("counter_value");

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
