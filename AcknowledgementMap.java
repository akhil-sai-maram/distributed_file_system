import java.net.Socket;
import java.util.List;

public class AcknowledgementMap {

  private String filename;
  private int n;

  private Socket socket;
  private int port;
  private List<Integer> ints;

  public AcknowledgementMap(String f, int n, Socket s, int p, List<Integer> ints) {
    this.filename = f;
    this.n = n;
    this.socket = s;
    this.port = p;
    this.ints = ints;
  }

  public String getFilename() {
    return filename;
  }

  public int getN() {
    return n;
  }

  public Socket getSocket() {
    return socket;
  }

  public int getPort() {
    return port;
  }

  public List<Integer> getInts() {
    return ints;
  }
}
