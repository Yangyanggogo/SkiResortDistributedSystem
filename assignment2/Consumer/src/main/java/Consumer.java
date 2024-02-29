import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;


public class Consumer {
  private ConnectionFactory connectionFactory;
  private Integer numberThreads = 64;

  private Connection conn;

  private final String QUEUE_NAME = "PostMessageQUEUE";

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

      //connectionFactory.setHost("localhost");

      } catch (IOException | TimeoutException e) {
        e.printStackTrace();

      }


  }

  public static void main(String[] args) {
    int numberThreads = 64; // Default value
    int basicqos = 10;
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
      multiThreadPool.execute(new ConsumerRunnable(consumer.getQUEUE_NAME(), consumer.getConn(),basicqos));
    }

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
