import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;

public class ConsumerRunnable implements Runnable{

  private String queueName;
  private Connection conn;
  private int basicqos;

  //private final DynamoDbClient dynamoDbClient;

  private List<WriteRequest> writeRequests = new ArrayList<>();
  private final DynamoDBBatchWriter batchWriter;
  Gson gson = new Gson();
  public ConsumerRunnable(String queueName, Connection conn, int basicqos, DynamoDBBatchWriter batchWriter){
    this.queueName = queueName;
    this.conn = conn;
    this.basicqos = basicqos;
    this.batchWriter = batchWriter;
//    this.dynamoDbClient = DynamoDbClient.builder()
//        .region(Region.US_WEST_2)
//        .build();
  }

  @Override
  public void run() {
    try {
      Channel channel = conn.createChannel();
      channel.queueDeclare(queueName,false,false, false, null);
      channel.basicQos(basicqos);

      DeliverCallback deliverCallback = (consumerTag, delivery)->{
        String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
        JsonObject json = gson.fromJson(msg, JsonObject.class);
        Map<String, AttributeValue> item = new HashMap<>();
        String skierID = json.get("skierID").getAsString();
        String resortID = json.get("resortID").getAsString();
        String compositeKey = createCompositeKey(json);

        item.put("skierID", AttributeValue.builder().n(skierID).build());
        item.put("compositeKey", AttributeValue.builder().s(compositeKey).build());
        item.put("resortID", AttributeValue.builder().n(resortID).build());
        batchWriter.bufferWriteRequest(item);

      };
      channel.basicConsume(this.queueName, true ,deliverCallback, consumerTag -> {});

    } catch (IOException e) {
      System.out.println("Exception");
      e.printStackTrace();
    }

  }


//  private void writeToDynamoDB(JsonObject json) {
//    Map<String, AttributeValue> item = new HashMap<>();
//    String skierID = json.get("skierID").getAsString();
//    String resortID = json.get("resortID").getAsString();
//    String compositeKey = createCompositeKey(json);
//
//    item.put("skierID", AttributeValue.builder().n(skierID).build());
//    item.put("compositeKey", AttributeValue.builder().s(compositeKey).build());
//    item.put("resortID", AttributeValue.builder().n(resortID).build());
//
//    PutItemRequest request = PutItemRequest.builder()
//        .tableName("SkierActivities")
//        .item(item)
//        .build();
//
//    try {
//      dynamoDbClient.putItem(request);
//      //System.out.println("Data written to DynamoDB for skierID: " + skierID);
//    } catch (Exception e) {
//      System.err.println("Error writing to DynamoDB for skierID: " + skierID + " - " + e.getMessage());
//      e.printStackTrace();
//    }
//  }

  private String createCompositeKey(JsonObject json) {
    String seasonID = json.get("seasonID").getAsString(); // Make sure these keys exist in your JSON
    String dayID = json.get("dayID").getAsString();
    String time = json.get("time").getAsString();
    String liftID = json.get("liftID").getAsString();

    return seasonID + "#" + dayID + "#" + time + "#" + liftID;
  }



}
