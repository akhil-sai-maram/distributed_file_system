import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class Dstore {

  /**
   * Parameters for constructor
   */
  private final int port;
  private final int cport;
  private final int timeout;

  /**
   * Folder for dstore files
   */
  private String folder;

  /**
   * File index
   */
  private final HashMap<String,Integer> file_sizes;

  /**
   * Controller objects
   */
  private Socket socketController;
  private BufferedReader readerController;
  private PrintWriter writerController;

  public Dstore(int port, int cport, int timeout, String fileFolder) {
    this.cport = cport;
    this.port = port;
    this.timeout = timeout;
    this.folder = fileFolder;
    this.file_sizes = new HashMap<>();

    deleteDirContent(new File(fileFolder));

    try {
      DstoreLogger.init(LogIndex.LogType.ON_FILE_AND_TERMINAL);
    } catch (IOException e) {
      System.out.println("DSTORE LOGGER ALREADY INITIALISED");
    }
  }

  /**
   * Entry point for creating Dstore
   * @param args dstorePort, controllerPort, timeout, folderName
   */
  public static void main(String[] args) {
    Dstore dstore = new Dstore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]),args[3]);
    DstoreLogger.getInstance().log("CREATING DSTORE WITH PORT " + args[0]);
    dstore.start();
  }

  /**
   * Notify controller of JOIN and start listening for messages from client and controller
   */
  public void start() {
    while (true) {
      if (joinController()) break;
    }
    System.out.println(String.format("[DSTORE %S]: OPENING SOCKETS AND SERVER SOCKETS",port));
    new Thread(this::clientRequests).start();
    new Thread(this::controllerRequests).start();
  }

  /**
   * Start thread for listening to messages from client
   */
  public void clientRequests() {
    while (true) {
      try (ServerSocket ss = new ServerSocket(port)) {
        Socket client = ss.accept();
        DstoreLogger.getInstance().connectionAccepted(port);
        new Thread(() -> clientActionHandler(client)).start();
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
    }
  }

  /**
   * Handle commands from client
   * @param socket client socket
   */
  private void clientActionHandler(Socket socket) {
    InputStream in;
    OutputStream out;

    try {
      in = socket.getInputStream();
      out = socket.getOutputStream();

      BufferedReader reader = new BufferedReader(new InputStreamReader(in));

      String line = reader.readLine();
      DstoreLogger.getInstance().messageReceived(socket.getPort(),line);
      switch (line.split(" ")[0]) {
        case Protocol.STORE_TOKEN -> storeClient(socket, in, out, line);
        case Protocol.LOAD_DATA_TOKEN -> loadClient(out, line);
        case Protocol.REBALANCE_STORE_TOKEN -> rebalanceStoreClient(socket,in,out,line);
        case Protocol.REMOVE_TOKEN -> removeDStore(socket, line);
        //case Protocol.LIST_TOKEN -> listController();
        case Protocol.REBALANCE_TOKEN -> rebalanceOperation(line);
        default -> System.out.println("NO RECOGNISED COMMAND: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Handle commands from controller
   */
  public void controllerRequests() {
    while (true) {
      try {
        String line = readerController.readLine();
        DstoreLogger.getInstance().messageReceived(cport,line);

        new Thread(() -> {
          switch (line.split(" ")[0]) {
            case Protocol.REMOVE_TOKEN -> removeDStore(socketController,line);
            case Protocol.LIST_TOKEN -> listController();
            case Protocol.REBALANCE_TOKEN -> rebalanceOperation(line);
            default -> System.out.println("UNABLE TO RECEIVE COMMAND FROM CONTROLLER");
          }
        }).start();
      } catch (IOException e) {
        DstoreLogger.getInstance().log("TIMEOUT OCCURED AT DSTORE " + port);
      }
    }
  }

  /**
   * Handle store request from client
   * @param socket client socket
   * @param in input stream
   * @param out output stream
   * @param line command
   */
  public void storeClient(Socket socket, InputStream in, OutputStream out, String line) {
    if (line.split(" ").length != 3) {
      System.out.println("INVALID COMMAND FORMAT SPECIFIED");
      return;
    }

    String filename = line.split(" ")[1];
    int filesize = Integer.parseInt(line.split(" ")[2]);
    String filepath = folder + File.separator + filename;
    File file = new File(filepath);

    try (FileOutputStream fos = new FileOutputStream(file)) {
      DstoreLogger.getInstance().log("WRITING CONTENTS TO FILE: " + filename);

      PrintWriter writer = new PrintWriter(out);
      DstoreLogger.getInstance().log("[DSTORE]: SENDING ACK TOKEN TO CLIENT");

      writer.println(Protocol.ACK_TOKEN);
      writer.flush();
      DstoreLogger.getInstance().messageSent(socket.getPort(),Protocol.ACK_TOKEN);

      fos.write(in.readNBytes(filesize));
      file_sizes.put(filename,filesize);

      DstoreLogger.getInstance().log("[DSTORE]: EXECUTING STORE COMMAND FROM CLIENT");

      writerController.println(Protocol.STORE_ACK_TOKEN + " " + filename);
      writerController.flush();

      DstoreLogger.getInstance().messageSent(socketController.getPort(),
          Protocol.STORE_ACK_TOKEN + " " + filename);

      writer.close();

    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("UNABLE TO STORE FILE: " + filename);
      file_sizes.remove(filename);
    }
  }

  /**
   * Handle load request from client
   * @param out output stream
   * @param line command
   */
  public void loadClient(OutputStream out, String line) {
    if (line.split(" ").length != 2) {
      System.out.println("ERROR IN RECEIVED LOAD COMMAND");
      return;
    }
    String filename = line.split(" ")[1];
    String filepath = folder + File.separator + filename;
    File f = new File(filepath);

    try (FileInputStream fis = new FileInputStream(f)) {
      DstoreLogger.getInstance().log("SENDING CONTENT OF FILE TO CLIENT: " + filename);
      if (f.exists()) {
        byte[] content = new byte[1024];
        int l;
        while ((l = fis.read(content)) != -1) {
          out.write(content,0,l);
        }
        out.flush();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Handle LIST command from controller
   */
  public void listController() {
    String message = Protocol.LIST_TOKEN + " ";

    DstoreLogger.getInstance().log("BUILDING LIST MESSAGE");

    message += (file_sizes.keySet().size() == 0) ? ""
        : String.join(" ", file_sizes.keySet());

    synchronized (writerController) {
      writerController.println(message);
      writerController.flush();
      DstoreLogger.getInstance().messageSent(cport, message);
    }
  }

  /**
   * Handle REBALANCE command from client
   * @param line command
   */
  public void rebalanceOperation(String line) {
    if (line.split(" ").length < 2) {System.out.println("ERROR - INVALID MESSAGE FORMAT"); return;}
    try {
      var rebalanceInfo = Rebalancer.rebalanceParser(line);

      if (rebalanceInfo[0] == null) {
        DstoreLogger.getInstance().log("NO FILES TO STORE IN REBALANCE OPERATION");
      } else {
        rebalanceStoreDStore(rebalanceInfo[0]);
      }

      if (rebalanceInfo[1] == null) {
        DstoreLogger.getInstance().log("NO FILES TO REMOVE IN REBALANCE OPERATION");
      } else {
        rebalanceRemoveDStore(rebalanceInfo[1]);
      }

      Thread.sleep(100);
      writerController.println(Protocol.REBALANCE_COMPLETE_TOKEN);
      writerController.flush();
      DstoreLogger.getInstance().messageSent(cport,Protocol.REBALANCE_COMPLETE_TOKEN);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("ERROR IN HANDLING REBALANCE OPERATION");
    }
  }

  /**
   * Execute remove command as part of rebalance operations
   * @param m files to remove
   */
  private void rebalanceRemoveDStore(HashMap<String,List<String>> m) {
    //m should only have one list containing files to remove
    if (m.size() != 1) {System.out.println("MALFORMED INPUT FOR REBALANCE_REMOVE OPERATION"); return;}

    if (m.containsKey("0")) {
      System.out.println("NO FILES TO REMOVE IN REBALANCE OPERATION");
      return;
    }

    List<String> files = m.entrySet().stream().findFirst().get().getValue();
    files.forEach(f -> {
      File fr = new File(folder+File.separator+f);
      try {
        if (!fr.exists()) {
          try {
            throw new FileNotFoundException(f);
          } catch (FileNotFoundException e) {
            DstoreLogger.getInstance().log(String.format("ERROR: FILE %s DOES NOT EXIST AT DSTORE %s",f,port));
          }
        } else {
          ControllerLogger.getInstance().log("DELETING FILE: " + fr.getName() + " IN DSTORE " + port);
          file_sizes.remove(f);
          fr.delete();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  /**
   * Execute store command as part of dstore operations
   * @param socket controller socket
   * @param in input stream
   * @param out output stream
   * @param line command
   */
  public void rebalanceStoreClient(Socket socket, InputStream in, OutputStream out, String line) {
    if (line.split(" ").length != 3) {
      System.out.println("INVALID COMMAND FORMAT SPECIFIED FOR REBALANCE STORE OPERATION");
      return;
    }

    String filename = line.split(" ")[1];

    int filesize;
    try {
      filesize = Integer.parseInt(line.split(" ")[2]);
    } catch (NumberFormatException e) {
      System.out.println("MALFORMED ARGUMENT FOR REBALANCE STORE");
      return;
    }

    String filepath = folder + File.separator + filename;
    File file = new File(filepath);

    try (FileOutputStream fos = new FileOutputStream(file)) {
      System.out.println("WRITING CONTENTS TO FILE: " + filename);

      PrintWriter writer = new PrintWriter(out);
      System.out.println("[DSTORE]: SENDING ACK TOKEN TO CLIENT");

      writer.println(Protocol.ACK_TOKEN);
      writer.flush();
      out.flush();
      DstoreLogger.getInstance().messageSent(socket.getPort(),Protocol.ACK_TOKEN);

      var temp = in.readNBytes(filesize);
      fos.write(temp);
      file_sizes.put(filename,filesize);

      DstoreLogger.getInstance().log("[DSTORE]: EXECUTING REBALANCE_STORE COMMAND FROM CLIENT");

      writerController.println(Protocol.REBALANCE_COMPLETE_TOKEN + " " + filename);
      writerController.flush();

      DstoreLogger.getInstance().messageSent(socketController.getPort(),
          Protocol.STORE_ACK_TOKEN + " " + filename);

      writer.close();

    } catch (IOException e) {
      System.out.println("UNABLE TO STORE FILE WHILE REBALANCING: " + filename);
      file_sizes.remove(filename);
    }
  }

  /**
   * Send store requests to other dstores as part of rebalance operations
   * @param files files to send
   * @return boolean indicating success of operation
   */
  public boolean rebalanceStoreDStore(HashMap<String,List<Integer>> files) {
    List<Boolean> tasks = new ArrayList<>();

    files.forEach((f,ps) -> ps.forEach(p -> tasks.add(rebalanceStoreSender(f,p))));

    try {
      long failedOperations = tasks.stream().map(b -> {
        try { return b; } catch (Exception e) {return false;}
      }).filter(b -> !b).count();

      return (int) failedOperations == 0;

    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }

  }

  /**
   * Handle receipt of storing as part of rebalance operations
   * @param filename file to store
   * @param dPort destination port
   * @return indicator of success
   */
  public boolean rebalanceStoreSender(String filename, int dPort) {

    try (Socket dstore = new Socket(InetAddress.getLocalHost(), dPort)) {
      dstore.setSoTimeout(timeout);
      System.out.println("EXECUTING REBALANCE STORE");
      var out = dstore.getOutputStream();
      PrintWriter pw = new PrintWriter(out);
      BufferedReader br = new BufferedReader(new InputStreamReader(dstore.getInputStream()));

      FileInputStream fis = new FileInputStream(folder+File.separator+filename);
      String message = String.format("%s %s %s",Protocol.REBALANCE_STORE_TOKEN,filename,file_sizes.get(filename));
      pw.println(message);
      pw.flush();
      DstoreLogger.getInstance().messageSent(dPort,message);

      String received = br.readLine();
      DstoreLogger.getInstance().messageReceived(dPort,received);

      if (received.equals(Protocol.ACK_TOKEN)) {
        byte[] bytes = new byte[1024];
        int l;

        while ((l = fis.read(bytes)) != -1) {
          out.write(bytes, 0, l);
        }
        fis.close();

        return true;
      } else { DstoreLogger.getInstance().log("ACK MESSAGE NOT RECEIVED - SUSPENDING REBALANCE OEPRATION");
        return false;
      }
    } catch (FileNotFoundException e) {
      DstoreLogger.getInstance().log(String.format("ERROR: FILE %s DOES NOT EXIST AT DSTORE %s",filename,port));
      return false;
    } catch (IOException e) {
      System.out.println("ERROR IN REBALANCE STORE OPERATION");
      return false;
    }
  }

  /**
   * Handle REMOVE request from controller
   * @param socket controller
   * @param line command
   */
  public void removeDStore(Socket socket, String line) {

    if (line.split(" ").length != 2) {
      System.out.println("ERROR REMOVING FILE IN DSTORE");
      return;
    }

    DstoreLogger.getInstance().messageReceived(socket.getPort(),line);
    String filename = line.split(" ")[1];

    String filepath = folder + File.separator + filename;
    File f = new File(filepath);

    String message = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename;
    if (f.exists()) {
      f.delete();
      System.out.println("DELETING FILE: " + filename);
      file_sizes.remove(filename);
      message = Protocol.REMOVE_ACK_TOKEN + " " + filename;
    }

    synchronized (writerController) {
      writerController.println(message);
      writerController.flush();
    }

    DstoreLogger.getInstance().messageSent(cport,message);
  }

  /**
   * Initial operations when joining controller
   * @return boolean to indicate join success
   */
  public boolean joinController() {
    try {
      Socket socket = new Socket(InetAddress.getLocalHost(),cport);
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      PrintWriter writer = new PrintWriter(socket.getOutputStream(),true);

      String message = Protocol.JOIN_TOKEN + " " + port;
      writer.println(message);
      writer.flush();
      DstoreLogger.getInstance().messageSent(cport,message);

      socketController = socket;
      readerController = reader;
      writerController = writer;
      DstoreLogger.getInstance().log("SUCCESSFULLY JOINED TO CONTROLLER");
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Delete directory
   * @param dir folder
   */
  private void deleteDir(File dir) {
    File[] files = dir.listFiles();

    if (files == null) {
      dir.delete();
    } else {
      Arrays.stream(files).forEach(this::deleteDir);
    }
  }

  /**
   * Delete content in directory
   * @param dir folder
   */
  private void deleteDirContent(File dir){
    Arrays.stream(Objects.requireNonNull(dir.listFiles())).forEach(this::deleteDir);
  }

  public int getPort() {
    return port;
  }

  public String getFolder() {
    return folder;
  }

  public String setFolder(String f) {
    folder = f;
    return folder;
  }

  public HashMap<String, Integer> getFile_sizes() {
    return file_sizes;
  }
}
