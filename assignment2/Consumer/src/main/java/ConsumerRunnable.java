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
import java.util.concurrent.CopyOnWriteArrayList;
public class ConsumerRunnable implements Runnable{

  private String queueName;
  private Connection conn;
  private int basicqos;
  Gson gson = new Gson();
  public ConsumerRunnable(String queueName, Connection conn, int basicqos){
    this.queueName = queueName;
    this.conn = conn;
    this.basicqos = basicqos;
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
        //System.out.println(json);

        Integer skierID = json.get("skierID").getAsInt();
        try {

          doConsume(skierID,json);
        } finally {
          System.out.println(Thread.currentThread().getId() + " - thread received " + json);
          //channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
      };
      channel.basicConsume(this.queueName, true ,deliverCallback, consumerTag -> {});

    } catch (IOException e) {
      System.out.println("Exception");
      e.printStackTrace();
    }

  }

    private void doConsume(int skierID, JsonObject json) {
      try {
        if(Consumer.record.containsKey(skierID)){

          Consumer.record.get(skierID).add(json);
        } else {

          List<JsonObject> newRecord = Collections.synchronizedList(new ArrayList<>());
          newRecord.add(json);
          Consumer.record.put(skierID, newRecord);
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Exception doConsumer");
      }
    }



}
