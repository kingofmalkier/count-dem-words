import com.datastax.driver.core.*;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import java.util.HashMap;
import java.util.Map;

public class WordCountPersistence {
    private Session session;
    private PreparedStatement countForWord;
    private PreparedStatement incrementCountForWord;
    private PreparedStatement deleteWordFromTopTen;
    private PreparedStatement topTenForTitle;
    private PreparedStatement addToTopTen;
    private PreparedStatement addNewTitleWithWordCount;

    public Map<String, Integer> getTopTenByTitle(String bookTitle) {
        Row result = getSession().execute(topTenForTitle.bind(bookTitle)).one();
        return result.getMap("counts", String.class, Integer.class);
    }

    private void replaceTopTen(String oldWord, String newWord, int count, String title) {
        getSession().execute(deleteWordFromTopTen.bind(oldWord, title));

        addToTopTen(newWord, count, title);
    }

    public long getCurrentCountForWord(String word, String title) {
        ResultSet results = getSession().execute(countForWord.bind(word, title));

        if (!results.isExhausted()) {
            return results.one().getLong("counter_value");
        }

        return 0;
    }

    public void incrementCountForWord(String word, String title) {
        getSession().execute(incrementCountForWord.bind(word, title));
    }

    public void updateTopTen(String word, int currentCount, String title) {
        ResultSet results = getSession().execute(topTenForTitle.bind(title));
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
        Map<String, Integer> countMap = new HashMap<>();
        countMap.put(word, (int)currentCount);
        getSession().execute(addNewTitleWithWordCount.bind(countMap, title));
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

    private void addToTopTen(String word, int count, String title) {
        getSession().execute(addToTopTen.bind(word, count, title));
    }

    private Session getSession() {
        if (session == null) {
            session = connect("127.0.0.1");
            countForWord = session.prepare("SELECT * FROM total_word_counts WHERE word_name = ? AND title = ?;");
            incrementCountForWord = session.prepare("UPDATE total_word_counts SET counter_value = counter_value + 1 " +
                    "WHERE word_name = ? AND  title = ?;");
            deleteWordFromTopTen = session.prepare("DELETE counts[?] FROM top_ten_words WHERE title = ?;");
            topTenForTitle = session.prepare("SELECT * FROM top_ten_words WHERE title = ?;");
            addToTopTen = session.prepare("UPDATE top_ten_words SET counts[?] = ? WHERE title = ?;");
            addNewTitleWithWordCount = session.prepare("UPDATE top_ten_words SET counts = ? WHERE title = ?;");
        }

        return session;
    }

    private Session connect(String node) {
        return Cluster.builder()
                .addContactPoint(node).withPort(9142)
                .build().connect("wordsKS"); //TODO: Hardcoded?
    }
}
