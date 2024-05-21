import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Rebalancer {

  public static HashMap<Integer, List<String>> getFilesToRetrieve(
      HashMap<Integer, List<String>> filesAtDstores, int R) {
    List<String> allFilesUnique =
        filesAtDstores.values().stream()
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toList());

    // Calculate min and max files to store
    int N = filesAtDstores.size();
     int F = allFilesUnique.size();
     double exactLimit = (R*F) / (N * 1.0);
     int limitU = (int) Math.ceil(exactLimit);
     int limitL = (int) Math.floor(exactLimit);

    List<String> allFiles = new ArrayList<>();

    for (int i = 0; i < R; i++) {
      allFiles.addAll(allFilesUnique);
    }

    List<List<String>> filesSplit = splitList(allFiles, N);
    HashMap<Integer, List<String>> filesAtDstoresNew = new HashMap<>();

    var ports = new ArrayList<>(filesAtDstores.keySet());
    for (int i = 0; i < filesAtDstores.size(); i++) {
      filesAtDstoresNew.put(ports.get(i), filesSplit.get(i));
    }

//    while (filesAtDstoresNew.values().stream().map(e -> e.size()).noneMatch(e -> e >= limitL || e <= limitU)) {
//      getFilesToRetrieve(filesAtDstoresNew,R);
//    }
    System.out.println("before" + filesAtDstores);
    System.out.println("after" + filesAtDstoresNew);

    return filesAtDstoresNew;
  }

  public static <T> List<List<T>> splitList(List<T> list, int n) {
    List<List<T>> result = new ArrayList<>();
    int size = list.size();
    int chunkSize = size / n;
    int remainder = size % n;
    int offset = 0;

    for (int i = 0; i < n; i++) {
      int chunkLength = chunkSize + (remainder-- > 0 ? 1 : 0);
      result.add(list.subList(offset, offset + chunkLength));
      offset += chunkLength;
    }

    return result;
  }

  public static HashMap<Integer, List<String>>[] getFilesToAddAndRemove(
      HashMap<Integer, List<String>> oldFiles, HashMap<Integer, List<String>> newFiles) {
    // Create the hashmaps to store the files to add and remove
    HashMap<Integer, List<String>> filesToAdd = new HashMap<>();
    HashMap<Integer, List<String>> filesToRemove = new HashMap<>();

    // Loop through each port in the new files hashmap
    for (Map.Entry<Integer, List<String>> entry : newFiles.entrySet()) {
      int port = entry.getKey();
      List<String> newPortFiles = entry.getValue();
      List<String> oldPortFiles = oldFiles.get(port);

      // If the old files hashmap doesn't contain this port, add all the new files for this port
      if (oldPortFiles == null) {
        filesToAdd.put(port, newPortFiles);
        continue;
      }

      // Loop through each file in the new port files list
      for (String file : newPortFiles) {
        // If the old port files list doesn't contain this file, add it to the files to add hashmap
        if (!oldPortFiles.contains(file)) {
          filesToAdd.computeIfAbsent(port, k -> new ArrayList<>()).add(file);
        }
      }

      // Loop through each file in the old port files list
      for (String file : oldPortFiles) {
        // If the new port files list doesn't contain this file, add it to the files to remove
        // hashmap
        if (!newPortFiles.contains(file)) {
          filesToRemove.computeIfAbsent(port, k -> new ArrayList<>()).add(file);
        }
      }
    }

    return new HashMap[] {filesToAdd, filesToRemove};
  }

  public static String rebalanceMessageBuilder(
      HashMap<String, List<Integer>> sendmap, HashMap<Integer, List<String>> removemap, int port) {
    var temp = reformat(sendmap);
    temp.remove(port);
    HashMap<String,List<Integer>> send = revert(temp);

    int sendCount = send.size();

    List<String> remove = removemap.get(port);
    int removeCount = (remove == null) ? 0 : remove.size();

    String message = Protocol.REBALANCE_TOKEN + " " + sendCount + " ";

    List<String> sendString = new ArrayList<>();

    send.forEach(
        (key, value) ->
            sendString.add(
                key
                    + " "
                    + value.size()
                    + " "
                    + value.stream()
                        .map(p -> Integer.toString(p))
                        .collect(Collectors.joining(" "))));

    String removeString = (removeCount != 0) ? removeCount + " " + String.join(" ", remove) : "0";

    message += String.join(" ", sendString) + " " + removeString;

    return message;
  }

  public static HashMap[] rebalanceParser(String line) {
    line = line.substring(Protocol.REBALANCE_TOKEN.length() + 1);

    HashMap[] sendAndRemove = new HashMap[2];
    sendAndRemove[0] = rebalanceHelperSend(line);
    sendAndRemove[1] = rebalanceHelperRemove(line);

    return sendAndRemove;
  }

  private static HashMap<String, List<String>> rebalanceHelperRemove(String line) {
    List<String> temp = new ArrayList<>();
    Collections.addAll(temp, line.split(" "));
    if (temp.get(0) == "0") {
      return null;
    }

    HashMap<String, List<String>> removeFiles = new HashMap<>();

    Collections.reverse(temp);

    int n = 0;

    for (int i = 0; i < temp.size(); i++) {
      try {
        Integer value = Integer.parseInt(temp.get(i));
        if (value != null) n = i;
        break;
      } catch (NumberFormatException e) {
        continue;
      }
    }

    temp = temp.stream().limit(n + 1).collect(Collectors.toList());
    Collections.reverse(temp);

    String fileCount = temp.get(0);
    temp.remove(0);

    removeFiles.put(fileCount, temp);

    return removeFiles;
  }

  private static HashMap rebalanceHelperSend(String line) {
    if (line.startsWith("0")) return null;

    ArrayList<String> lineSplit = new ArrayList<>();
    Collections.addAll(lineSplit, line.split(" "));

    int count = Integer.parseInt(lineSplit.get(0));
    lineSplit.remove(0);

    HashMap<String, List<Integer>> fileAndPorts = new HashMap();

    for (int i = 1; i <= count; i++) {
      String file = lineSplit.get(0);

      lineSplit.remove(0);

      int countPorts = Integer.parseInt(lineSplit.get(0));

      lineSplit.remove(0);
      List<Integer> tempPorts = new ArrayList<>();

      for (int j = 0; j < countPorts; j++) {
        tempPorts.add(Integer.parseInt(lineSplit.get(0)));
        lineSplit.remove(0);
      }

      fileAndPorts.put(file, tempPorts);
    }

    return fileAndPorts;
  }

  public static HashMap<Integer, List<String>> reformat(
      HashMap<String, List<Integer>> dstoreFileAllocations) {
    HashMap<Integer, List<String>> filesAtDstores = new HashMap<>();

    List<Integer> allPorts =
        dstoreFileAllocations.values().stream()
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toList());

    allPorts.forEach(p -> filesAtDstores.put(p, new ArrayList<>()));

    dstoreFileAllocations
        .keySet()
        .forEach(f -> dstoreFileAllocations.get(f).forEach(p1 -> filesAtDstores.get(p1).add(f)));

    return filesAtDstores;
  }

  public static HashMap<String, List<Integer>> revert(
      HashMap<Integer, List<String>> filesAtDstores) {
    HashMap<String, List<Integer>> fileMap = new HashMap<>();

    List<String> files =
        filesAtDstores.values().stream()
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toList());

    files.forEach(f -> fileMap.put(f, new ArrayList<>()));

    filesAtDstores
        .keySet()
        .forEach(p1 -> filesAtDstores.get(p1).forEach(f -> fileMap.get(f).add(p1)));

    return fileMap;
  }
}
