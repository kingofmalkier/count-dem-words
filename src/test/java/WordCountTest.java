import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.Vector;

public class WordCountTest {
    @Test
    public void ultraTrivialBook() {
        Vector<String> ultraTrivalLines = new Vector<String>();
        ultraTrivalLines.add("This is my only line. This is fun!");

        WordCount wc = new WordCount();
        wc.countWords("Trivial", ultraTrivalLines);

        Integer thisCount = wc.topTenWords().get("this");
        assertEquals(Integer.valueOf(2), thisCount);
    }
}
