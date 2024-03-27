import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBBatchWriter {

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;
  private final List<WriteRequest> writeRequests;
  private final int batchSize;
  private final int maxRetryAttempts;
  private final int baseBackoffTime;

  private long successfulWritesCount = 0; // Counter for successful writes



  public DynamoDBBatchWriter(DynamoDbClient dynamoDbClient, String tableName, int batchSize, int maxRetryAttempts, int baseBackoffTime) {
    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
    this.batchSize = batchSize;
    this.maxRetryAttempts = maxRetryAttempts;
    this.baseBackoffTime = baseBackoffTime;
    this.writeRequests = new ArrayList<>();
  }

  public synchronized void bufferWriteRequest(Map<String, AttributeValue> item) {
    writeRequests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());

    if (writeRequests.size() >= batchSize) {
      flush();
    }
  }

  public synchronized void flush() {
    if (writeRequests.isEmpty()) {
      return;
    }

    Map<String, List<WriteRequest>> requestItems = new HashMap<>();
    requestItems.put(tableName, new ArrayList<>(writeRequests));

    int attempts = 0;
    BatchWriteItemResponse response = null;

    do {
      BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
          .requestItems(requestItems)
          .build();

      response = dynamoDbClient.batchWriteItem(batchWriteItemRequest);
      requestItems = response.unprocessedItems();

      if (!requestItems.isEmpty()) {
        try {
          Thread.sleep((long) Math.pow(2, attempts) * baseBackoffTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return; // Early exit if the thread's interrupted status is set
        }
        attempts++;
      }
    } while (attempts < maxRetryAttempts && !requestItems.isEmpty());


    if (!response.unprocessedItems().isEmpty()) {
      logUnprocessedItems(response.unprocessedItems());
      successfulWritesCount += writeRequests.size() - requestItems.values().stream().mapToInt(List::size).sum();
    } else {
      successfulWritesCount += writeRequests.size();
    }

    writeRequests.clear();
  }

  private void logUnprocessedItems(Map<String, List<WriteRequest>> unprocessedItems) {
    // Implement logging here, such as sending alerts or logging to a file or monitoring system
    System.err.println("Unprocessed items after retries: " + unprocessedItems);
  }
  public synchronized long getSuccessfulWritesCount() {
    return successfulWritesCount;
  }
}
