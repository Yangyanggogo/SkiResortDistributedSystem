import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.atomic.AtomicLong;



public class ConsumerRunnable implements Runnable{

  private String queueName;
  private Connection conn;
  private int basicqos;

  private JedisPool jedisPool;
  private static final AtomicLong successfulMessagesCount = new AtomicLong(0);


  Gson gson = new Gson();
  public ConsumerRunnable(String queueName, Connection conn, int basicqos, JedisPool jedisPool){
    this.queueName = queueName;
    this.conn = conn;
    this.basicqos = basicqos;
    this.jedisPool = jedisPool;

  }

  @Override
  public void run() {

    try {
      Channel channel = conn.createChannel();
      channel.queueDeclare(queueName,false,false, false, null);
      channel.basicQos(basicqos);

      DeliverCallback deliverCallback = (consumerTag, delivery)->{
        String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);

        storeMessages(msg);

      };
      channel.basicConsume(this.queueName, true ,deliverCallback, consumerTag -> {});

    } catch (IOException e) {
      System.out.println("Exception");
      e.printStackTrace();
    }

  }

  private void storeMessages(String msg) {
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();

      JsonObject json = gson.fromJson(msg, JsonObject.class);
      String skierId = json.get("skierID").getAsString();
      String seasonId = json.get("seasonID").getAsString();
      String dayId = json.get("dayID").getAsString();
      String liftId = json.get("liftID").getAsString();


      Map<String, String> current = jedis.hgetAll(skierId);
      current.put("seasonID", current.getOrDefault("seasonID", "") + "|" + seasonId);
      current.put("dayID", current.getOrDefault("dayID", "") + "|" + dayId);
      current.put("liftID", current.getOrDefault("liftID", "") + "|" + liftId);


      jedis.hmset(skierId, current);
      successfulMessagesCount.incrementAndGet();

    }catch (Exception e) {
      System.err.println("Exception in storeMessages: " + e.getMessage());
      e.printStackTrace();
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }

  }

  public static long getSuccessfulMessagesCount() {
    return successfulMessagesCount.get();
  }



}
