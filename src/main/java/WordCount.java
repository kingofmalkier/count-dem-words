import java.util.Map;



/** A service that counts words in books. */
public class WordCount {
    private static final String GRAND_TOTAL = "----grand-total----";
    private WordCountPersistence persistence;

    public WordCount() {
        persistence = new WordCountPersistence();
    }

    /**
     * Provides the top ten most common words found cumulatively across all
     * books that have been processed by the service.
     *
     * @return A mapping of at most ten words to their respective counts. Never
     *         null, but possibly empty.
     */
    public Map<String, Integer> topTenWords() {
        return topTenWords(GRAND_TOTAL);
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

        return persistence.getTopTenByTitle(bookTitle);
    }

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
            word = word.replaceAll("[^a-zA-Z]+$", "");
            if (word.isEmpty()) {
                continue;
            }
            countWordForGrandTotal(word);
            countWordForTitle(word, bookTitle);
        }
    }

    private void countWordForTitle(String word, String title) {
        int currentCount = persistence.getCurrentCountForWord(word, title);
        persistence.incrementCountForWord(word, title);
        persistence.updateTopTen(word, currentCount + 1, title);
    }

    private void countWordForGrandTotal(String word) {
        countWordForTitle(word, GRAND_TOTAL);
    }
}
