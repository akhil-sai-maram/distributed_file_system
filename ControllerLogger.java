import java.io.IOException;

public class ControllerLogger extends LogIndex {

  private static final String LOG_FILE_SUFFIX2 = "[CONTROLLER]:";
  private static final String LOG_FILE_SUFFIX = "controller";

  private static ControllerLogger instance = null;

  public static ControllerLogger getInstance() {
    if (instance == null) throw new RuntimeException("Controller Logger has not been initialised");
    return instance;
  }

  public static void resetInstance() {
    instance = null;
  }

  public static void init(LogIndex.LogType type) throws IOException {
    if (instance != null) throw new IOException("Controller Logger already initialised");
    instance = new ControllerLogger(type);
  }

  public ControllerLogger(LogIndex.LogType type) {
    super(type);
  }

  public void storeToSent(String ports, int cport) {
    String portsf = String.join("\n", ports.split(" "));
    this.log(
        String.format(
            "%s Sending STORE_TO command to %s on ports: %s",
            LOG_FILE_SUFFIX2, cport, String.join(" ", portsf)));
  }

  public void receivingStoreAck(int port, String filename) {
    this.log(
        String.format("%s Receiving STORE_ACK for %s from %s", LOG_FILE_SUFFIX2, filename, port));
  }

  public void storeCompleteSent(int cport, String filename) {
    this.log(
        String.format(
            "%s Sent STORE_COMPLETE to client at port %s for file %s",
            LOG_FILE_SUFFIX2, cport, filename));
  }

  public void listReceived(Boolean client) {
    String end = client ? "client" : "dstore";
    this.log(LOG_FILE_SUFFIX2 + " Receiving LIST from " + end);
  }

  public void listSent(Boolean client) {
    String end = client ? "client" : "dstore";
    this.log(LOG_FILE_SUFFIX2 + " Sending LIST of existing files to " + end);
  }

  @Override
  protected String getLogFileSuffix() {
    return LOG_FILE_SUFFIX;
  }

  public static String getLogFileSuffix2() {
    return LOG_FILE_SUFFIX2;
  }
}
