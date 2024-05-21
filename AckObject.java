import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class AckObject {
  private final AtomicInteger count;
  private final Socket socket;
  private final long filesize;
  private final List<Integer> values;

  public AckObject(int count, Socket socket, long filesize, List<Integer> values) {
    this.count = new AtomicInteger(count);
    this.socket = socket;
    this.filesize = filesize;
    this.values = new ArrayList<>(values);
  }

  public int getCount() {
    return count.get();
  }

  public void incrementCount() {
    count.incrementAndGet();
  }

  public Socket getSocket() {
    return socket;
  }

  public long getFilesize() {
    return filesize;
  }

  public List<Integer> getValues() {
    return new ArrayList<>(values);
  }

  public void addValue(int value) {
    values.add(value);
  }

  public void addValues(List<Integer> values) {
    this.values.addAll(values);
  }
}
