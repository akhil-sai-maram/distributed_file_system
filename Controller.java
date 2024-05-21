import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Controller {

  /**
   * Parameters for controller object
   */
  private int cport;
  private int timeout;
  private int R;
  private int rebalancePeriod;

  /**
   * Store the file and the size of file
   */
  private final Map<String,FileObject> file_list;

  /**
   * Store active dstores with sockets
   */
  private final Map<Integer,Socket> dstoreSockets;

  /**
   * Sockets required for reload operations
   */
  private final Map<Socket,List<Integer>> suspendedReloads;

  /**
   * Lists for storing inactive and active dstores
   */
  private final List<Socket> inactiveDstores;
  private final List<Integer> dstorePorts;

  /**
   * File Index storing dstores and their contained files
   * precedingAllocation used to store updated file index from rebalance
   */
  private Map<String, List<Integer>> dstoreFileAllocations;
  private Map<Integer,List<String>> predecingAllocation;

  /**
   * List of acknowledgements
   */
  private final List<AcknowledgementMap> acknowledgements;

  //filename,acknowledgementCount,SocketClient,messageIDs
  /**
   * Hashmap containing filename, acknowledges received, client socket, ID of messages
   */
  private final Map<String,AckObject> acknowledges;

  /**
   * Boolean variables to track which operations are in progress
   */
  private AtomicBoolean storeInProgress;
  private AtomicBoolean removeInProgress;
  private AtomicBoolean rebalanceInProgress;
  private boolean rebalancePossible;

  /**
   * Entry point of building Controller class
   * @param args controllerPort, Replication Factor, Timeout for receiving acknowledgements, rebalancePeriod
   */
  public static void main(String[] args) {
    Controller controller = new Controller(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    controller.start();
  }

  public Controller(int port, int R, int timeout, int rebalancePeriod) {
    this.cport = port;
    this.R = R;
    this.timeout = timeout;
    this.rebalancePeriod = rebalancePeriod;

    this.dstorePorts = Collections.synchronizedList(new ArrayList<>());
    this.dstoreSockets = Collections.synchronizedMap(new HashMap<>());

    this.file_list = Collections.synchronizedMap(new HashMap<>());
    this.dstoreFileAllocations = Collections.synchronizedMap(new HashMap<>());

    this.suspendedReloads = Collections.synchronizedMap(new HashMap<>());

    this.acknowledgements = new ArrayList<>();
    this.acknowledges = Collections.synchronizedMap(new HashMap<>());

    this.inactiveDstores = Collections.synchronizedList(new ArrayList<>());

    storeInProgress = new AtomicBoolean(false);
    removeInProgress = new AtomicBoolean(false);
    rebalanceInProgress = new AtomicBoolean(false);
    rebalancePossible = true;

    try {
      ControllerLogger.init(LogIndex.LogType.ON_FILE_AND_TERMINAL);
    } catch (IOException e) {
      ControllerLogger.getInstance().log("CONTROLLER LOGGER ALREADY INITIALISED");
    }
  }

  /**
   * Run threads for scheduling rebalance operations and listen for client messages
   */
  public synchronized void start() {
    new Thread(this::scheduleRebalance).start();
    new Thread(this::incomingClientRequests).start();
  }

  /**
   * Start a thread for listening for requests from clients
   */
  private void incomingClientRequests() {
    ServerSocket ss = null;

    try {
      ss = new ServerSocket(cport);

      while (true) {
        Socket client = ss.accept();
        ControllerLogger.getInstance().connectionAccepted(client.getPort());
        new Thread(() -> messageHandler(client)).start();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally{
      try {
        ss.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Command handler that calls relevant method based on line read
   * @param client
   */
  private void messageHandler(Socket client) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
      PrintWriter writer = new PrintWriter(client.getOutputStream());
      ControllerLogger.getInstance().connectionEstablished(client.getPort());

      String line;

      while ((line = reader.readLine()) != null) {
        while (rebalanceInProgress.get()) {}

        String finalLine = line;
        ControllerLogger.getInstance().messageReceived(client.getPort(),line);
        switch (line.split(" ")[0]) {
          case Protocol.STORE_TOKEN -> new Thread(() -> storeClient(client, writer, finalLine)).start();
          case Protocol.LIST_TOKEN -> new Thread(() -> listClient(writer, client)).start();
          case Protocol.LOAD_TOKEN -> new Thread(() -> loadClient(writer, client, finalLine)).start();
          case Protocol.REMOVE_TOKEN -> new Thread(() -> removeClient(writer, client, finalLine)).start();
          case Protocol.RELOAD_TOKEN -> new Thread(() -> reloadClient(writer, client, finalLine)).start();
          case Protocol.JOIN_TOKEN -> new Thread(() -> joinClient(client, finalLine)).start();
          case Protocol.STORE_ACK_TOKEN -> new Thread((() -> storeAckDStore(finalLine,client))).start();
          case Protocol.REMOVE_ACK_TOKEN -> new Thread(() -> removeAckDStore(finalLine)).start();
          case Protocol.REBALANCE_COMPLETE_TOKEN -> rebalanceComplete();
          default -> System.out.printf("INVALID COMMAND RECEIVED FROM PORT %s: %s%n",client.getPort(), finalLine);
        }
      }

    } catch (IOException e) {
      suspendedReloads.remove(client);
      e.printStackTrace();
    }
  }

  /**
   * Handle JOIN requests from Dstore
   * @param socket socket of dstore
   * @param line JOIN command
   */
  private void joinClient(Socket socket, String line) {
    if (line.split(" ").length != 2) {
      System.out.println("ERROR: INVALID ARGUMENTS FOR OPERATION");
      return;
    }

    int port;

    //Attempt to parse integer port value from message
    try {
      port = Integer.parseInt(line.split(" ")[1]);
    } catch (NumberFormatException e) {
      System.out.println("MALFORMED VALUE FOR PORT");
      return;
    }

    //Add dstore to relevant methods and initialise rebalance operations and listen for dstore requests
    if (!dstorePorts.contains(port)) {
      ControllerLogger.getInstance().messageSent(socket.getPort(),line);
      dstorePorts.add(port);
      dstoreSockets.put(port,socket);
      new Thread(() -> incomingDStoreRequests(socket)).start();

      if (dstorePorts.size() >= R) {
        System.out.println("STARTING REBALANCE CHECKS");

        new Thread(() -> {while (!rebalanceOperation()) {}}).start();
      }
    } else {
      System.out.println("DSTORE AT PORT "+ port + " ALREADY EXISTS");
    }
  }

  /**
   * Have rebalance execute periodically based on time given in constructor
   */
  private void scheduleRebalance() {
    TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        if (dstorePorts.size() >= R) {rebalanceOperation();}
      }
    };

    new Timer().schedule(timerTask,0,rebalancePeriod);
  }

  /**
   * Ensure safe execution of rebalance and trigger method to send LIST
   * @return
   */
  private boolean rebalanceOperation() {
    if (dstoreFileAllocations.size() != 0 && rebalancePossible && rebalanceInProgress.compareAndSet(false,true)) {
      while (storeInProgress.get() || removeInProgress.get()) {}

      ControllerLogger.getInstance().log("STARTING REBALANCE OPERATIONS");
      sendListDstore();
      return true;
    }

    return (dstoreFileAllocations.size() == 0);
  }

  /**
   * Send a LIST command to each dstore
   */
  private void sendListDstore() {
    dstorePorts.parallelStream().forEach(p ->
        {
          try {
            Socket ds = dstoreSockets.get(p);
            //Socket ds = new Socket(InetAddress.getLocalHost(),p);
            PrintWriter pw = new PrintWriter(ds.getOutputStream());
            pw.println(Protocol.LIST_TOKEN);
            pw.flush();
            ControllerLogger.getInstance().messageSent(p,Protocol.LIST_TOKEN);

            ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
            ses.schedule(() -> {
              new Thread(this::rebalanceContinuation).start();
            },timeout, TimeUnit.MILLISECONDS);
          } catch (Exception e) {e.printStackTrace();}
        }
    );
  }

  /**
   * Method to revise file allocation and track changes required
   */
  private void rebalanceContinuation() {
    file_list.keySet().forEach(f -> {
      if (!dstoreFileAllocations.containsKey(f)) file_list.remove(f);
    });

    if (dstoreFileAllocations.size() == 0) {
      System.out.println("FILE ALLOCATIONS HAS NO ENTRIES");
      rebalanceInProgress.set(false);
      return;
    }

    var allocatedPorts = Rebalancer.reformat(new HashMap<>(dstoreFileAllocations));
    dstorePorts.forEach(p -> {
      if (!allocatedPorts.containsKey(p)) {
        allocatedPorts.put(p,new ArrayList<>());
      }
    });
    //Updated hashmap of new file allocations
    predecingAllocation = Rebalancer.getFilesToRetrieve(allocatedPorts,R);

    //Identify changes between old and new file allocation, and store in relevant hashmaps
    var maps = Rebalancer.getFilesToAddAndRemove(Rebalancer.reformat(new HashMap<>(dstoreFileAllocations)),
        new HashMap<>(predecingAllocation));
    var filesSend = maps[0];
    var filesRemove = maps[1];

    if (predecingAllocation.size() == 0) {
      rebalanceInProgress.set(false);
      return;
    }

    dstorePorts.forEach(p -> rebalanceDstore(Rebalancer.revert(filesSend), Rebalancer.revert(filesRemove),p));
  }

  /**
   * Send REBALANCE command to relevant dstore
   * @param send files to send
   * @param remove files to remove
   * @param port destination port
   */
  private void rebalanceDstore(HashMap<String, List<Integer>> send, HashMap<String, List<Integer>> remove, int port) {
    String message = Rebalancer.rebalanceMessageBuilder(send,Rebalancer.reformat(remove), port);

    if (message.equals("REBALANCE 0 0")) {
      System.out.println("NO REBALANCING PROCEDURES REQUIRED");
      rebalanceInProgress.set(false);
      return;
    }

    try {
      Socket s = new Socket(InetAddress.getLocalHost(),port);
      PrintWriter writer = new PrintWriter(s.getOutputStream());
      writer.println(message);
      writer.flush();
      ControllerLogger.getInstance().messageSent(s.getPort(), message);

    } catch (Exception e) {
      dstoreSockets.remove(port);

      Iterator<Integer> it = dstorePorts.iterator();
      int i = 0;
      while (it.hasNext()) {
        i++;
        if (it.next() == port) break;
      }

      dstorePorts.remove(i % dstorePorts.size());
      e.printStackTrace();
    }
  }

  /**
   * Handle LIST response from Dstore
   * @param socket dstore
   * @param line command
   */
  private void receivelistDStore(Socket socket, String line) {
    if (line.equals(Protocol.LIST_TOKEN + " ")) {
      System.out.println("DSTORE " + socket.getPort() + " CURRENTLY STORES NO FILES");
    }

    ControllerLogger.getInstance().log("UPDATING FILE ALLOCATION");

    System.out.println("v1" + dstoreFileAllocations);

    //dstoreFileAllocations maps a filename to all ports that store it
    //Update file allocation with list received from dstores
    synchronized (dstoreFileAllocations) {
      Arrays.stream(line.split(" ")).skip(1).collect(Collectors.toList()).forEach(f -> {
        if (!f.equals("blank")) {
          if (dstoreFileAllocations.containsKey(f)) {
            var temp = dstoreFileAllocations.get(f);
            temp = temp.stream().distinct().collect(Collectors.toList());
            dstoreFileAllocations.put(f,temp);
          } else {
            dstoreFileAllocations.put(f,new ArrayList<>());
          }
        }
      });
    }

    System.out.println("v2" + dstoreFileAllocations);

    ControllerLogger.getInstance().listReceived(false);
    restartRebalance();
  }

  /**
   * Start rebalance operations again
   */
  private void restartRebalance() {
    if (dstorePorts.size() == 0){ return;}
    ControllerLogger.getInstance().log("RESTARTING REBALANCE");
    new Thread(this::rebalanceContinuation).start();
  }

  /**
   * Handle STORE command from client
   * @param socket client
   * @param writer
   * @param line
   */
  private void storeClient(Socket socket, PrintWriter writer, String line) {
    storeInProgress.set(true);

    if (line.split(" ").length != 3) {
      storeInProgress.set(false);
      System.out.println("ERROR: INVALID ARGUMENTS FOR OPERATION");
      return;
    }

    String filename = line.split(" ")[1];

    int filesize;

    //Attempt to extract file size from command
    try {
      filesize = Integer.parseInt(line.split(" ")[2]);
    } catch (NumberFormatException e) {
      System.out.println("MALFORMED ARGUMENT FOR STORE OPERATION: EXPECTED INTEGER");
      storeInProgress.set(false);
      return;
    }

    ControllerLogger.getInstance().log("RETRIEVING PORTS OF DSTORES FOR STORING FILE :" + filename);

    if (dstorePorts.size() < R) {
      storeInProgress.set(false);
      writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(),Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }

    if (dstoreFileAllocations.containsKey(filename) || acknowledges.containsKey(filename)) {
      storeInProgress.set(false);
      ControllerLogger.getInstance().log("SUSPENDING STORE OPERATION: " + Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      writer.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(),Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
      return;
    }

    file_list.put(filename,new FileObject(filename,Status.store_in_progress,filesize));

    //Get all ports to store file in
    String ports = dstorePorts.stream().map(p -> Integer.toString(p))
        .collect(Collectors.joining(" "));
    String message = Protocol.STORE_TO_TOKEN + " " + ports;
    ControllerLogger.getInstance().storeToSent(ports,cport);
    writer.println(message);
    writer.flush();

    acknowledges.put(filename, new AckObject(0,socket,filesize,dstorePorts));

    acknowledgements.add(new AcknowledgementMap(filename,0,socket,filesize,dstorePorts));
  }

  /**
   * Handle LIST command from client
   * @param writer Printwriter
   * @param socket client socket
   */
  private void listClient(PrintWriter writer, Socket socket) {
    if (dstorePorts.size() < R) {
      writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(),Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }

    String fileList;
    synchronized (dstoreFileAllocations) {
      if (dstoreFileAllocations.size() != 0) {
        List<String> files = dstoreFileAllocations.keySet().stream().distinct().collect(Collectors.toList());
        files.removeAll(acknowledges.keySet());
        fileList = String.join(" ", files);
      } else {
        fileList = "";
      }
    }

    writer.println(Protocol.LIST_TOKEN + " " + fileList);
    writer.flush();
    ControllerLogger.getInstance().listSent(true);
  }

  /**
   * Handle REMOVE command from Client
   * @param writer Printwriter
   * @param socket Client socket
   * @param line command
   */
  private void removeClient(PrintWriter writer, Socket socket, String line) {
    removeInProgress.set(true);
    if (line.split(" ").length != 2) {
      removeInProgress.set(false);
      ControllerLogger.getInstance()
          .messageSent(socket.getPort(), "ERROR: INVALID ARGUMENTS FOR OPERATION");
      return;
    }

    String filename = line.split(" ")[1];
    ControllerLogger.getInstance().messageReceived(socket.getPort(),line);
    ControllerLogger.getInstance().log("REMOVE OPERATION IN PROGRESS FOR FILE " + filename);

    if (dstorePorts.size() < R) {
      removeInProgress.set(false);
      writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(),Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }


    if (!(dstoreFileAllocations.containsKey(filename) && (!acknowledges.containsKey(filename)))) {
      removeInProgress.set(false);
      writer.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(), Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }

    file_list.get(filename).setStatus(Status.remove_in_progress);

    List<Integer> dstores = new ArrayList<>(dstoreFileAllocations.get(filename));
    acknowledges.put(filename, new AckObject(0,socket,0,dstores));

    dstorePorts.forEach(p -> removeDStore(p,filename));

    //Schedule new thread to track number of ACKs received
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.schedule(()->{
      if (acknowledges.containsKey(filename)) {
        removeInProgress.set(false);
        System.out.printf("ERROR: DELAYED RECEIPT OF REMOVE_ACK TOKEN%n");
        acknowledges.remove(filename);
      }
    }, timeout,TimeUnit.MILLISECONDS);
  }

  /**
   * Send REMOVE command to dstore
   * @param portDStore destination port
   * @param filename file to remove
   */
  private void removeDStore(int portDStore, String filename) {
    try {
      Socket socket = dstoreSockets.get(portDStore);
      //Socket socket = new Socket(InetAddress.getLocalHost(),portDStore);
      PrintWriter writer = new PrintWriter(socket.getOutputStream());

      String message = Protocol.REMOVE_TOKEN + " " + filename;
      writer.println(message);
      writer.flush();
      ControllerLogger.getInstance().messageSent(portDStore,message);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Handle LOAD request from client
   * @param writer Printwriter
   * @param socket client socket
   * @param line command
   */
  private void loadClient(PrintWriter writer, Socket socket, String line) {
    if (line.split(" ").length != 2) {
      System.out.println("ERROR IN MESSAGE FORMAT AND ARGUMENTS");
      return;
    }

    String filename = line.split(" ")[1];

    if (dstorePorts.size() < R) {
      writer.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(),Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
      return;
    }

    if (file_list.get(filename).getStatus() == Status.remove_complete || file_list.get(filename).getStatus() == Status.remove_in_progress) {
      writer.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(),Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
      return;
    }

    //Determine ports to handle reload operations
    suspendedReloads.put(socket,dstoreFileAllocations.get(filename).stream().skip(1).collect(Collectors.toList()));
    String message = Protocol.LOAD_FROM_TOKEN + " " + dstoreFileAllocations.get(filename).get(0) + " " + file_list.get(filename).getFilesize();
    writer.println(message);
    writer.flush();
    ControllerLogger.getInstance().messageSent(socket.getPort(), message);
  }

  /**
   * Handle RELOAD command from client
   * @param writer Printwriter
   * @param socket client socket
   * @param line command
   */
  private void reloadClient(PrintWriter writer, Socket socket, String line) {
    if (line.split(" ").length != 2) {
      System.out.println("ERROR IN RELOADING TOKEN");
      return;
    }

    if (!suspendedReloads.containsKey(socket)) {
      ControllerLogger.getInstance().log("RELOAD OPERATION ONLY VALID AFTER LOAD OPERATION");
      return;
    }
    String filename = line.split(" ")[1];

    int size = file_list.get(filename).getFilesize();

    //Get first socket to retry loading, and remove it after message is sent
    String message;
    if(suspendedReloads.get(socket).size()==0){
      suspendedReloads.remove(socket);
      writer.println(Protocol.ERROR_LOAD_TOKEN); writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(), Protocol.ERROR_LOAD_TOKEN);
    }else{
      int port = suspendedReloads.get(socket).get(0);
      suspendedReloads.get(socket).remove(0);
      message = Protocol.LOAD_FROM_TOKEN + " " + port + " " + size;
      writer.println(message); writer.flush();
      ControllerLogger.getInstance().messageSent(socket.getPort(), message);
    }

  }

  /**
   * Message handler for Dstore requests
   * @param socket dstore socket
   */
  private void incomingDStoreRequests(Socket socket) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      socket.setSoTimeout(timeout);

      String line;
      while (true) {
        line = reader.readLine();
        if (line == null) throw new IOException();
        String finalLine = line;
        ControllerLogger.getInstance().messageReceived(socket.getPort(),finalLine);
        new Thread(() -> {
          switch (finalLine.split(" ")[0]) {
            case Protocol.STORE_ACK_TOKEN -> new Thread(() -> storeAckDStore(finalLine, socket)).start();
            case Protocol.REMOVE_ACK_TOKEN -> new Thread(() -> removeAckDStore(finalLine)).start();
            case Protocol.LIST_TOKEN -> {
              ControllerLogger.getInstance().listReceived(false);
              new Thread(() -> receivelistDStore(socket, finalLine)).start();
            }
            case Protocol.REBALANCE_COMPLETE_TOKEN -> rebalanceComplete();
            case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> new Thread(() -> nonexistentFileError(finalLine)).start();
            default -> ControllerLogger.getInstance().log("INVALID COMMAND RECEIVED: " + finalLine);
          }
        }).start();
      }
    } catch (IOException e) {
      boolean t = false;
      if (inactiveDstores.contains(socket)) {
        System.out.println("SOCKET AT PORT " + socket.getPort() + " INACTIVE");
        inactiveDstores.remove(socket);
        t = true;
      }
      dstoreDisconnection(t,socket);
    }
  }

  /**
   * Update file allocations once rebalance complete
   */
  private void rebalanceComplete() {
    ControllerLogger.getInstance().log("REBALANCING OPERATIONS COMPLETE - UPDATING FILE ALLOCATION");
    dstoreFileAllocations = (predecingAllocation != null) ? Rebalancer.revert(new HashMap<>(predecingAllocation)) : dstoreFileAllocations;
    predecingAllocation = null;
    rebalanceInProgress.set(false);
  }

  /**
   * Handle cases where file does not exist
   * @param line
   */
  private void nonexistentFileError(String line) {
    if (line.split(" ").length != 2) {ControllerLogger.getInstance().log("ERROR IN HANDLING ERROR MESSAGE"); return;}

    String filename = line.split(" ")[1];

    if (!acknowledges.containsKey(filename)) {
      System.out.println("ERROR: DELAYED RECEIPT OF FILE_DOES_NOT_EXIST TOKEN");
      return;
    }

    AckObject temp = acknowledges.get(filename);
    temp.incrementCount();
    if (temp.getCount() >= temp.getValues().size()) {
      removeInProgress.set(false);
      removeCompleteClient(temp.getSocket());
      dstoreFileAllocations.remove(filename);
      file_list.remove(filename);
      acknowledges.remove(filename);
    }
  }

  /**
   * Handle STORE_ACK from dstore
   * @param line command
   * @param socket dstore
   */
  private void storeAckDStore(String line, Socket socket) {
    if (line.split(" ").length != 2) {
      System.out.println("ERROR RECEIVING STORE_ACK");
      return;
    }

    String filename = line.split(" ")[1];

    ControllerLogger.getInstance().receivingStoreAck(socket.getPort(),filename);

    if (!acknowledges.containsKey(filename)) {
      System.out.println("ERROR: DELAYED RECEIPT OF STORE_ACK TOKEN");
      return;
    }

    AckObject temp = acknowledges.get(filename);
    temp.incrementCount();
    if (temp.getCount() == R) {
      storeInProgress.set(false);

      storeCompleteClient(temp.getSocket());
      dstoreFileAllocations.put(filename,temp.getValues());
      file_list.get(filename).setStatus(Status.store_complete);
      acknowledges.remove(filename);

    }
  }

  /**
   * Finish Store operation from client
   * @param s client
   */
  private void storeCompleteClient(Socket s) {
    try {
      PrintWriter pw = new PrintWriter(s.getOutputStream());
      pw.println(Protocol.STORE_COMPLETE_TOKEN);
      ControllerLogger.getInstance().messageSent(s.getPort(),Protocol.STORE_COMPLETE_TOKEN);
      pw.flush();
    } catch (IOException e) {e.printStackTrace();}
  }


  /**
   * Handle REMOVE_ACK from dstore
   * @param line command
   */
  private void removeAckDStore(String line) {

    if (line.split(" ").length != 2) {
      System.out.println("ERROR REMOVING FILE");
      return;
    }

    String filename = line.split(" ")[1];

    if(!acknowledges.containsKey(filename)){
      System.out.println("DELAYED RECEIPT OF REMOVE_ACK TOKEN");
      return;
    }

    acknowledges.get(filename).incrementCount();
    if(acknowledges.get(filename).getCount() >= acknowledges.get(filename).getValues().size()) {
      removeInProgress.set(false);
      removeCompleteClient(acknowledges.get(filename).getSocket());
      dstoreFileAllocations.remove(filename);
      file_list.get(filename).setStatus(Status.remove_complete);
      acknowledges.remove(filename);
    }

  }

  /**
   * Finish remove operation from client
   * @param s client socket
   */
  private void removeCompleteClient(Socket s) {
    try {
      PrintWriter pw = new PrintWriter(s.getOutputStream());
      pw.println(Protocol.REMOVE_COMPLETE_TOKEN);
      pw.flush();
      ControllerLogger.getInstance().messageSent(s.getPort(),Protocol.REMOVE_COMPLETE_TOKEN);
    } catch (IOException e) {e.printStackTrace();}
  }

  /**
   * Clean up properties when dstore disconnects
   * @param timeoutCaused if dstore disconnected from timeout
   * @param s dstore socket
   */
  private void dstoreDisconnection(boolean timeoutCaused,Socket s) {
    int p = (!dstoreSockets.values().contains(s)) ? 0 : s.getPort();
    dstorePorts.remove(p);
    dstoreSockets.remove(p);
    clearSuspendedReloads(p);
    clearAcknowledges(p);
    if (!timeoutCaused) {
      clearAllocatedFiles(p);
      clearFileList();
      restartRebalance();
    }
  }

  /**
   * Clean up file allocation
   * @param port dstore port
   */
  private void clearAllocatedFiles(int port) {
    synchronized (dstoreFileAllocations) {
      dstoreFileAllocations.keySet().forEach(f -> {
        dstoreFileAllocations.get(f).remove(port);
        if (dstoreFileAllocations.get(f).size() == 0) {
          dstoreFileAllocations.remove(f);
        }
      });

    }
  }

  /**
   * Clean up file tracker
   */
  private void clearFileList(){
    synchronized (file_list) {
      file_list.keySet().forEach(f -> {
        if (!dstoreFileAllocations.containsKey(f)) file_list.remove(f);
      });
    }
  }

  /**
   * Clean up acknowledge tracker
   * @param port dstore port
   */
  private void clearAcknowledges(int port){
    synchronized (acknowledges){
      for(var file : acknowledges.keySet()){
        acknowledges.get(file).getValues().remove((Integer)port);
      }
    }

  }

  /**
   * Clean up list for tracking reloads
   * @param port dstore port
   */
  private void clearSuspendedReloads(int port){
    synchronized (suspendedReloads){
      for(var socket : suspendedReloads.keySet()){
        suspendedReloads.get(socket).remove((Integer) port);
      }
    }
  }

  public void setRebalancePossible(boolean rebalancePossible) {
    this.rebalancePossible = rebalancePossible;
  }

  public int getTimeout() {
    return timeout;
  }

  public Map<String, List<Integer>> getDstoreFileAllocations() {
    return dstoreFileAllocations;
  }

  public int getR() {
    return R;
  }

  public List<Integer> getDstorePorts() {
    return dstorePorts;
  }

  public Map<String, AckObject> getAcknowledges() {
    return acknowledges;
  }

  public Map<String, FileObject> getFile_list() {
    return file_list;
  }

  public AtomicBoolean getRemoveInProgress() {
    return removeInProgress;
  }

  public AtomicBoolean getRebalanceInProgress() {
    return rebalanceInProgress;
  }
}

class FileObject {

  private String filename;
  private Status status;
  private Integer filesize;

  public FileObject(String filename, Status status, Integer filesize) {
    this.filename = filename;
    this.status = status;
    this.filesize = filesize;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public Integer getFilesize() {
    return filesize;
  }

  public void setFilesize(Integer filesize) {
    this.filesize = filesize;
  }
}

enum Status {
  store_in_progress,
  store_complete,
  remove_in_progress,
  remove_complete
}