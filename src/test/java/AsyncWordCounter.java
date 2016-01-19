import java.util.Vector;

public class AsyncWordCounter implements Runnable {
    public boolean continueCounting = false;

    @Override
    public void run() {
        WordCount wc = new WordCount();
        Vector<String> input = new Vector<>();
        input.add("Threading is kind of hard.");

        wc.countWords("Threading", input);
        while (!continueCounting) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        input.clear();
        input.add("When writing threading code, always use code reviews.");
        wc.countWords("Threading", input);
    }
}
