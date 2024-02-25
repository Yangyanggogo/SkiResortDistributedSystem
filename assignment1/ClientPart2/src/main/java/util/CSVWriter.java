package util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CSVWriter {
  public static void writeMetricsToCsv(String filePath, ConcurrentLinkedQueue<RequestMetric> queue) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
      RequestMetric metric;
      while ((metric = queue.poll()) != null) {
        writer.write(metric.toString());
        writer.newLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeRequestsPerSecondToCsv(String filePath, ConcurrentMap<Long, AtomicInteger> requestsPerSecond) {

    try (FileWriter writer = new FileWriter(filePath)) {
      writer.append("Timestamp,Requests\n");
      requestsPerSecond.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(entry -> {
            try {
              writer.append(entry.getKey().toString()).append(",").append(entry.getValue().toString()).append("\n");
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
      System.out.println("Successfully wrote requests per second to CSV.");
    } catch (IOException e) {
      System.out.println("Error writing requests per second to CSV.");
      e.printStackTrace();
    }
  }
}
