package util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

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
}
