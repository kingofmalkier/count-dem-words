import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.junit.Assert.assertEquals;

public class WordCountTest {
    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("simple.cql","wordsKS"));

    @Test
    public void prettyTrivialTest() {
        Vector<String> ultraTrivalLines = new Vector<>();
        ultraTrivalLines.add("This is my only line this is fun");

        WordCount wc = new WordCount();
        wc.countWords("Trivial", ultraTrivalLines);

        Map<String, Integer> topTen = wc.topTenWords();

        Integer thisCount = topTen.get("this");
        Integer funCount = topTen.get("fun");

        assertEquals(Integer.valueOf(2), thisCount);
        assertEquals(Integer.valueOf(1), funCount);
    }

    @Test
    public void topTenOnlyAcrossAllTitles() {
        WordCount wc = new WordCount();

        Vector<String> basicLines = new Vector<>();
        basicLines.add("This is my first line this is fun");
        basicLines.add("Oh my word there are more lines");

        wc.countWords("Basic", basicLines);

        basicLines.add("Addendum groovy is groovy groovy too");

        wc.countWords("Basic 2nd Edition", basicLines);

        Map<String, Integer> topTen = wc.topTenWords();

        Integer thisCount = topTen.get("this");
        Integer groovyCount = topTen.get("groovy");
        Integer isCount = topTen.get("is");

        assertEquals(10, topTen.size());
        assertEquals(Integer.valueOf(4), thisCount);
        assertEquals(Integer.valueOf(3), groovyCount);
        assertEquals(Integer.valueOf(5), isCount);
    }

    @Test
    public void topTenOnlyFromOneTitle() {
        Vector<String> linesWeCareAbout = new Vector<>();
        linesWeCareAbout.add("Cool words we care about");
        linesWeCareAbout.add("Man we are so cool");

        Vector<String> linesWeIgnore = new Vector<>();
        linesWeIgnore.add("These words are not cool We hate them.");

        WordCount wc = new WordCount();
        wc.countWords("We Care", linesWeCareAbout);
        wc.countWords("Do not Care", linesWeIgnore);

        Integer coolCount = wc.topTenWords("We Care").get("cool");
        assertEquals(Integer.valueOf(2), coolCount);
    }

    @Test
    public void countNullTitle() {
        exceptionRule.expect(IllegalArgumentException.class);
        new WordCount().countWords(null, new Vector<String>());
    }

    @Test
    public void countEmptyTitle() {
        exceptionRule.expect(IllegalArgumentException.class);
        new WordCount().countWords("", new Vector<String>());
    }

    @Test
    public void countNullLines() {
        exceptionRule.expect(IllegalArgumentException.class);
        new WordCount().countWords("Neat Title", null);
    }

    @Test
    public void topTenNullTitle() {
        exceptionRule.expect(IllegalArgumentException.class);
        new WordCount().topTenWords(null);
    }

    @Test
    public void topTenEmptyTitle() {
        exceptionRule.expect(IllegalArgumentException.class);
        new WordCount().topTenWords("");
    }

    @Test
    public void asynchronousCounting() throws InterruptedException {
        AsyncWordCounter asyncCounter = new AsyncWordCounter();
        Thread asyncThread = new Thread(asyncCounter);
        asyncThread.start();
        Thread.sleep(500);

        WordCount wc = new WordCount();
        Integer threadingCount = wc.topTenWords().get("threading");
        assertEquals(Integer.valueOf(1), threadingCount);

        asyncCounter.continueCounting = true;
        asyncThread.join();

        threadingCount = wc.topTenWords().get("threading");
        Integer codeCount = wc.topTenWords().get("code");

        assertEquals("Times we counted 'threading'", Integer.valueOf(2), threadingCount);
        assertEquals("Times we counted 'code'", Integer.valueOf(2), codeCount);
    }

    @Test
    public void trailingPunctuation() {
        Vector<String> punctuatedWords = new Vector<>();
        punctuatedWords.add("This is obvious but do we count THIS? And this; or even this!");

        WordCount wc = new WordCount();
        wc.countWords("Trivial", punctuatedWords);

        Map<String, Integer> topTen = wc.topTenWords();

        Integer thisCount = topTen.get("this");

        assertEquals("'This' count with punctuation.", Integer.valueOf(4), thisCount);
    }

    @Test
    public void infixPunctuation() {
        Vector<String> punctuatedWords = new Vector<>();
        punctuatedWords.add("It's timey-wimey?");
        punctuatedWords.add("Yes. Timey-wimey, wibbly-wobbly.");

        WordCount wc = new WordCount();
        wc.countWords("Infix", punctuatedWords);

        Map<String, Integer> topTen = wc.topTenWords();

        Integer twCount = topTen.get("timey-wimey");

        assertEquals(Integer.valueOf(2), twCount);
    }

    @Test
    public void dangerousWords() {
        Vector<String> dangerousWords = new Vector<>();
        //Only punctuation in the middle of a word gets processed...
        dangerousWords.add("We must fle;e It's a tra!p");

        WordCount wc = new WordCount();
        wc.countWords("Ackbar's Memoir", dangerousWords);

        Map<String, Integer> topTen = wc.topTenWords("Ackbar's Memoir");

        Integer itsCount = topTen.get("it's");
        Integer traipCount = topTen.get("tra!p");

        assertEquals("'It's' count.", Integer.valueOf(1), itsCount);
        assertEquals("'Traip' count.", Integer.valueOf(1), traipCount);
    }

    @Ignore("Turned off for speed. Took 280 seconds on my machine.")
    @Test
    public void largeBookTest() throws IOException, URISyntaxException {
        Path sherlockTextPath = Paths.get(WordCountTest.class.getResource("/sherlock.txt").toURI());
        List<String> list = Files.readAllLines(sherlockTextPath, StandardCharsets.UTF_8);

        WordCount wc = new WordCount();
        wc.countWords("Sherlock Compilation", list);

        Map<String, Integer> topTen = wc.topTenWords();

        assertEquals("Probably at least ten words in all of Sherlock.", 10, topTen.size());
    }
}
