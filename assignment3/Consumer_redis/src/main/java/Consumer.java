import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import redis.clients.jedis.JedisPool;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Consumer {
  private ConnectionFactory connectionFactory;
  private Integer numberThreads = 200;

  private Connection conn;

  private final String QUEUE_NAME = "PostMessageQUEUE";

  private JedisPool jedisPool;

  private String redisHost = "172.31.16.207";

  public static final Map<Integer, List<JsonObject>> record = new ConcurrentHashMap<>();


  public Consumer() {
    connectionFactory = new ConnectionFactory();
    Properties properties = new Properties();
    this.jedisPool = new JedisPool(redisHost, 6379);

    System.out.println("Connecting...");
    try (InputStream input = getClass().getClassLoader().getResourceAsStream("rabbitmq.conf")) {
      if (input == null) {
        return;
      }

      properties.load(input);

      connectionFactory.setHost(properties.getProperty("rabbitmq.host"));
      connectionFactory.setPort(Integer.parseInt(properties.getProperty("rabbitmq.port")));
      connectionFactory.setUsername(properties.getProperty("rabbitmq.username"));
      connectionFactory.setPassword(properties.getProperty("rabbitmq.password"));
      conn = connectionFactory.newConnection();
      System.out.println("Connection succeed!");



      } catch (IOException | TimeoutException e) {
        e.printStackTrace();

      }


  }

  public static void main(String[] args) {
    int numberThreads = 200; // Default value
    int basicqos = 1;
    if (args.length > 0) {
      try {
        numberThreads = Integer.parseInt(args[0]);
        basicqos = Integer.parseInt(args[1]);
      } catch (NumberFormatException e) {
        System.err.println("Argument" + args[0] +" and "+ args[1] + " must be an integer.");
        System.exit(1);
      }
    }
    Consumer consumer = new Consumer();
    consumer.setNumberThreads(numberThreads);
    ExecutorService multiThreadPool = Executors.newFixedThreadPool(consumer.getNumberThreads());
    for (int i = 0; i<consumer.getNumberThreads(); i++) {

      multiThreadPool.execute(new ConsumerRunnable(consumer.getQUEUE_NAME(), consumer.getConn(), basicqos, consumer.jedisPool));
    }

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(() -> {
      long count = ConsumerRunnable.getSuccessfulMessagesCount();
      System.out.println("Successful Redis writes: " + count);
    }, 0, 1, TimeUnit.MINUTES);

    // shutdown the scheduler during application shutdown
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(800, TimeUnit.MILLISECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
      }
    }));
  }



  public Integer getNumberThreads() {
    return numberThreads;
  }

  public String getQUEUE_NAME() {
    return QUEUE_NAME;
  }

  public Connection getConn() {
    return conn;
  }

  public void setNumberThreads(Integer numberThreads) {
    this.numberThreads = numberThreads;
  }
}
