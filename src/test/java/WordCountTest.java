import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
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
        wc.countWords("Do not Care", linesWeIgnore); //TODO: This title would be an issue as a contraction...

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
}
