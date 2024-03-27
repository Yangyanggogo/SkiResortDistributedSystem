import Model.Constant;
import Model.LiftRide;
import Model.Message;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;


@WebServlet(name = "SkierServlet", value = "/skiers")
public class SkierServlet extends HttpServlet {

  Gson gson = new Gson();
  public final Integer NUM_CHANNEL = 20;
  private BlockingQueue<Channel> channelPool;

  private Connection conn;

  private final String QUEUE_NAME = "PostMessageQUEUE";

  @Override
  public void init() throws ServletException {
    super.init();
    Properties properties = new Properties();
    try (InputStream input = getClass().getClassLoader().getResourceAsStream("rabbitmq.conf")) {
      if (input == null) {
        throw new ServletException("Unable to load rabbitmq.properties");
      }
      properties.load(input);
      ConnectionFactory connectionFactory = new ConnectionFactory();
      connectionFactory.setHost(properties.getProperty("rabbitmq.host"));
      connectionFactory.setPort(Integer.parseInt(properties.getProperty("rabbitmq.port")));
      connectionFactory.setUsername(properties.getProperty("rabbitmq.username"));
      connectionFactory.setPassword(properties.getProperty("rabbitmq.password"));

      //connectionFactory.setHost("localhost");

      try {
        conn = connectionFactory.newConnection();
        channelPool = new LinkedBlockingQueue<>();
        for (int i = 0; i < NUM_CHANNEL; i++) {
          Channel channel = conn.createChannel();
          channel.queueDeclare(QUEUE_NAME, false, false, false, null);
          channelPool.add(channel);
        }

        System.out.println("Queue declared successfully");
        System.out.println("Declaring queue: " + QUEUE_NAME);

      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
        throw new ServletException("Failed to initialize RabbitMQ connection", e);
      } catch (NumberFormatException e) {
        throw new ServletException("Invalid port number in rabbitmq.properties", e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    // Close all channels in the pool
    channelPool.forEach(channel -> {
      if (channel.isOpen()) {
        try {
          channel.close();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    });

    // Optionally close the connection if it's no longer needed
    if (conn != null && conn.isOpen()) {
      try {
        conn.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }


  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    PrintWriter out = res.getWriter();
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();

    Message message = new Message("Get Successful!");
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      message.setMessage("Empty request!");
      res.getWriter().write(gson.toJson(message));
      return;
    }
    String[] urls = urlPath.split("/");
//    for (String each : urls) {
//      System.out.println(each);
//    }

    if (!validURL(urls)) {
      message.setMessage("Invalid Request!");
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(0);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    PrintWriter out = res.getWriter();
    res.setCharacterEncoding("UTF-8");
    res.setContentType("application/json");

    String urlPath = req.getPathInfo();

    Message message = new Message("POST Successful!");

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      message.setMessage("POST Request Not Valid!");
      res.getWriter().write(gson.toJson(message));

      return;
    }
    String[] urls = urlPath.split("/");


    if (!validURL(urls)) {
      message.setMessage("Invalid Request 0!");
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      res.getWriter().write(gson.toJson(message));
      return;
    }
    try {
      LiftRide liftRide = gson.fromJson(req.getReader(), LiftRide.class);
      int skierID = Integer.parseInt(urls[7]);
      int resortID = Integer.parseInt(urls[1]);
      int seasonID = Integer.parseInt(urls[3]);
      int dayID = Integer.parseInt(urls[5]);

      if (liftRide.getLiftID() == null || liftRide.getTime() == null
          || liftRide.getResortId() < 0) {
        message.setMessage("Invalid Request here!");
        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }
      JsonObject msg = new JsonObject();
      msg.add("time", new JsonPrimitive(liftRide.getTime()));
      msg.add("liftID", new JsonPrimitive(liftRide.getLiftID()));
      msg.add("skierID", new JsonPrimitive(skierID));
      msg.add("resortID",new JsonPrimitive(resortID));
      msg.add("seasonID", new JsonPrimitive(seasonID));
      msg.add("dayID", new JsonPrimitive(dayID));

      Channel channel = null;
      try{
        channel = channelPool.take();
      }catch (InterruptedException e) {
        message.setMessage("RMQ is not working!");
        res.getWriter().write(gson.toJson(message));
        e.printStackTrace();
      }
      if (channel!=null){
        channel.basicPublish("", QUEUE_NAME, null, msg.toString().getBytes());
        res.setStatus(HttpServletResponse.SC_OK);
        //System.out.println("Sent " + msg + " to rabbitmq");
        res.getWriter().write("Sent " + msg + " to rabbitmq");
        channelPool.add(channel);
      } else{
        res.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
      }
    } catch (Exception e) {
      e.printStackTrace();
      message.setMessage("Unsuccessful!");
      out.write(gson.toJson(message));
      out.flush();
    }
  }

   /**
     *
     * @param urls "/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}"
    *             /skiers/1/seasons/1/days/1/skiers/1
    *
     * @return
     */
  private boolean validURL(String[] urls) {
    if (urls.length == 8) {
      if (!urls[2].equals("seasons") || !urls[4].equals("days") || !urls[6].equals("skiers")) {
        //System.out.println("A");
        return false;
      }
      else if (!urls[1].chars().allMatch(Character::isDigit) || !urls[3].chars().allMatch(Character::isDigit) ||!urls[5].chars().allMatch(Character::isDigit)
          || !urls[7].chars().allMatch(Character::isDigit)) {
        //System.out.println("B");
        return false;
      }

      else if (Integer.parseInt(urls[1])< Constant.RESORT_ID_BEGIN|| Integer.parseInt(urls[1])>Constant.RESORT_ID_END || !urls[3].equals(Constant.SEASON_ID)||
          !urls[5].equals(Constant.DAY_ID)  || Integer.parseInt(urls[7])<Constant.SKIER_ID_BEGIN ||Integer.parseInt(urls[7])>Constant.SKIER_ID_END){

        return false;
      }

      try {
        for (int i = 1; i < 8; i += 2) {
          Integer.parseInt(urls[i]);
        }
        return true;
      } catch (Exception e) {

        return false;
      }
    }
    return false;

  }



}
