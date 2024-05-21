import java.io.IOException;

public class DstoreLogger extends LogIndex {

  private static final String LOG_FILE_SUFFIX2 = "[DSTORE]: ";
  private static final String LOG_FILE_SUFFIX = "dstore";

  private static DstoreLogger instance = null;

  protected DstoreLogger(LogIndex.LogType loggingType) {
    super(loggingType);
  }

  public static DstoreLogger getInstance() {
    if (instance == null) throw new RuntimeException("Dstore Logger has not been initialised");
    return instance;
  }

  public static void resetInstance() {
    instance = null;
  }

  public static void init(LogIndex.LogType type) throws IOException {
    if (instance != null) throw new IOException("Dstore Logger already initialised");
    instance = new DstoreLogger(type);
  }

  @Override
  public String getLogFileSuffix() {
    return LOG_FILE_SUFFIX;
  }

  public static String getLogFileSuffix2() {
    return LOG_FILE_SUFFIX2;
  }
}
