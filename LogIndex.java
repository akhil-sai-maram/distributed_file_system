//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.io.IOException;
import java.io.PrintStream;

public abstract class LogIndex {
  protected static final String ERROR_LOG_MSG_SUFFIX = "ERROR: ";
  protected final LogIndex.LogType type;
  protected PrintStream ps;

  protected LogIndex(LogIndex.LogType type) {
    this.type = type;
  }

  protected abstract String getLogFileSuffix();

  protected PrintStream getPrintStream() throws IOException {
    if (this.ps == null) {
      this.ps =
          new PrintStream(this.getLogFileSuffix() + "_" + System.currentTimeMillis() + ".log");
    }

    return this.ps;
  }

  protected boolean logToFile() {
    return this.type == LogIndex.LogType.ON_FILE_ONLY
        || this.type == LogIndex.LogType.ON_FILE_AND_TERMINAL;
  }

  protected boolean logToTerminal() {
    return this.type == LogIndex.LogType.ON_TERMINAL_ONLY
        || this.type == LogIndex.LogType.ON_FILE_AND_TERMINAL;
  }

  protected void log(String message) {
    if (this.logToFile()) {
      try {
        this.getPrintStream().println(message);
      } catch (Exception var3) {
        var3.printStackTrace();
      }
    }

    if (this.logToTerminal()) {
      System.out.println(message);
    }
  }

  public void connectionAccepted(int remotePort) {
    this.log("Connection accepted from port ".concat(String.valueOf(remotePort)));
  }

  public void connectionEstablished(int remotePort) {
    this.log("Connection established to port ".concat(String.valueOf(remotePort)));
  }

  public void messageSent(int destinationPort, String message) {
    this.log("Message sent to port " + destinationPort + ": " + message);
  }

  public void messageReceived(int sourcePort, String message) {
    this.log("Message received from port " + sourcePort + ": " + message);
  }

  public void timeoutExpiredWhileReading(int remotePort) {
    this.log("Timeout expired while reading from port ".concat(String.valueOf(remotePort)));
  }

  public void error(String message) {
    this.log("ERROR: ".concat(String.valueOf(message)));
  }

  public static enum LogType {
    NO_LOG,
    ON_TERMINAL_ONLY,
    ON_FILE_ONLY,
    ON_FILE_AND_TERMINAL;

    private LogType() {}
  }
}
