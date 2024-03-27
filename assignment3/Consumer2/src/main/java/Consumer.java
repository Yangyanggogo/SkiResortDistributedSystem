import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Consumer {
  private ConnectionFactory connectionFactory;
  private Integer numberThreads = 200;

  private Connection conn;

  private final String QUEUE_NAME = "PostMessageQUEUE";
  private DynamoDBBatchWriter batchWriter;


  public static final Map<Integer, List<JsonObject>> record = new ConcurrentHashMap<>();


  public Consumer() {
    connectionFactory = new ConnectionFactory();
    Properties properties = new Properties();

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

      DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(Region.US_WEST_2).build();
      batchWriter = new DynamoDBBatchWriter(dynamoDbClient, "SkierActivities", 25, 5, 50);

      //connectionFactory.setHost("localhost");

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
      multiThreadPool.execute(new ConsumerRunnable(consumer.getQUEUE_NAME(), consumer.getConn(),basicqos,consumer.batchWriter));
    }
    // Schedule a task to print the successful write count every minute
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      long successfulWrites = consumer.batchWriter.getSuccessfulWritesCount();
      System.out.println("Successful writes to DynamoDB: " + successfulWrites);
    }, 0, 1, TimeUnit.MINUTES);
    // Add shutdown hook to flush any remaining writes
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (consumer.batchWriter != null) {
        consumer.batchWriter.flush();
      }
      scheduledExecutorService.shutdown();
      try {
        if (!scheduledExecutorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
          scheduledExecutorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduledExecutorService.shutdownNow();
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
