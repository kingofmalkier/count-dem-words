import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Map;

public class WordCountPersistence {
    private Session session;

    public Map<String, Integer> getTopTenByTitle(String bookTitle) {
        //TODO: Hide the session detail at a minimum
        Row result = getSession().execute("SELECT * FROM top_ten_words WHERE title='" + bookTitle + "';").one();
        return result.getMap("counts", String.class, Integer.class);
    }

    public void replaceTopTen(String oldWord, String newWord, long count, String title) {
        getSession().execute("DELETE counts['" + oldWord + "'] FROM top_ten_words WHERE title = '" + title + "';");

        addToTopTen(newWord, count, title);
    }

    public long getCurrentCountForWord(String word, String title) {
        ResultSet results = getSession().execute("SELECT * FROM total_word_counts WHERE word_name='" + word + "' AND " +
                "title = '" + title + "';");

        if (!results.isExhausted()) {
            return results.one().getLong("counter_value");
        }

        return 0;
    }

    public void incrementCountForWord(String word, String title) {
        getSession().execute("UPDATE total_word_counts\n" +
                " SET counter_value = counter_value + 1\n" +
                " WHERE word_name='" + word + "' AND " +
                "title = '" + title + "';");
    }

    public void updateTopTen(String word, long currentCount, String title) {
        ResultSet results = getSession().execute("SELECT * FROM top_ten_words WHERE title='" + title + "';");
        if (results.isExhausted()) {
            //We don't have any entry yet for this title
            //so we can just skip straight to creating an entry for this title/word
            addNewTitleWithWordCount(title, word, currentCount);
            return;
        }

        Map <String, Integer> countMap = results.one().getMap("counts", String.class, Integer.class);

        String wordToReplace = findWordToReplace(countMap, word, currentCount);

        //If there are less than 10 in the top ten then we *are* adding
        if (countMap.size() < 10) {
            if (wordToReplace == null) {
                addToTopTen(word, currentCount, title);
            } else if (wordToReplace.equals(word)) {
                replaceTopTen(wordToReplace, word, currentCount, title);
            } else {
                addToTopTen(word, currentCount, title);
            }
        } else if (wordToReplace != null) {
            replaceTopTen(wordToReplace, word, currentCount, title);
        }
    }

    public void addNewTitleWithWordCount(String title, String word, long currentCount) {
        getSession().execute("UPDATE top_ten_words\n" +
                "  SET counts =\n" +
                "  { '" + word + "' : " + currentCount + " }\n" +
                "  WHERE title = '" + title + "';");
    }

    private String findWordToReplace(Map<String, Integer> countMap, String word, long currentCount) {
        String wordToReplace = null;
        int lowestCount = Integer.MAX_VALUE;

        for (String mapWord : countMap.keySet()) {
            if (word.equals(mapWord)) {
                return word;
            }

            int mapCount = countMap.get(mapWord);
            if (mapCount < currentCount && mapCount < lowestCount) {
                wordToReplace = mapWord;
                lowestCount = mapCount;
            }
        }

        return wordToReplace;
    }

    private void addToTopTen(String word, long count, String title) {
        getSession().execute("UPDATE top_ten_words SET counts['" + word + "'] = " + count + "\n" +
                "  WHERE title = '" + title + "';");
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
