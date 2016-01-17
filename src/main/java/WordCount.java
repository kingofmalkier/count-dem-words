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
    private static final String GRAND_TOTAL = "----grand-total----";

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
        String title = GRAND_TOTAL;
        ResultSet results = getSession().execute("SELECT * FROM top_ten_words WHERE title='" + title + "';");
        if (results.isExhausted()) {
            //We don't have any entry yet for this title
            //so we can just skip straight to creating an entry for this title/word
            addNewTitleWithWordCount(title, word, currentCount);
            return;
        }

        //TODO: Time to get our map, which is the actual count data, from there it may actually be sort of similar
        //to the old implementation?

        int numRowsInTopTen = 0;
        Map <String, Integer> countMap = results.one().getMap("counts", String.class, Integer.class);

        String wordToReplace = findWordToReplace(word, currentCount);

        //If there are less than 10 in the top ten then we *are* adding
        if (numRowsInTopTen < 10) {
            if (wordToReplace == null) {
                addToTopTen(word, currentCount);
            } else if (wordToReplace.equals(word)) {
                replaceTopTen(wordToReplace, word, currentCount);
            } else {
                addToTopTen(word, currentCount);
            }
        } else if (wordToReplace != null) {
            replaceTopTen(wordToReplace, word, currentCount);
        }
    }

    private void addNewTitleWithWordCount(String title, String word, long currentCount) {
        getSession().execute("UPDATE top_ten_words\n" +
                "  SET counts =\n" +
                "  { '" + word + "' : " + currentCount + " }\n" +
                "  WHERE title = '" + title + "';");
    }

    private String findWordToReplace(String word, long currentCount) {
        String wordToReplace = null;
        ResultSet results = getSession().execute("SELECT * FROM top_ten_words WHERE count < " + currentCount + " ALLOW FILTERING;");

        for (Row row : results) {
            String rowWord = row.getString("word_name");
            if (word.equals(rowWord)) {
                return word;
            }

            int rowCount = row.getInt("count");
            if (rowCount > currentCount) {
                return wordToReplace;
            }

            if (wordToReplace == null && currentCount > rowCount) {
                wordToReplace = rowWord;
            }
        }

        return wordToReplace;
    }

    private void replaceTopTen(String oldWord, String newWord, long count) {
        String title = "";
        getSession().execute("DELETE FROM top_ten_words where word_name = '" + oldWord + "' AND title = '" + title + "';");

        addToTopTen(newWord, count);
    }

    private void addToTopTen(String word, long count) {
        String title = "";
        getSession().execute("INSERT INTO " + "top_ten_words (word_name, title, count)\n" +
                " VALUES ('" + word + "', '" + title + "', " + count + ");");
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
