import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Vector;

public class WordCountTest {
    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void ultraTrivialTest() {
        Vector<String> ultraTrivalLines = new Vector<>();
        ultraTrivalLines.add("This is my only line this is fun");

        WordCount wc = new WordCount();
        wc.countWords("Trivial", ultraTrivalLines);

        Integer thisCount = wc.topTenWords().get("this");
        assertEquals(Integer.valueOf(2), thisCount);
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
        wc.countWords("Don't Care", linesWeIgnore);

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
