import Model.LiftRide;
import Model.Message;
import com.google.gson.Gson;
import java.io.PrintWriter;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;


@WebServlet(name = "SkierServlet", value = "/skiers")
public class SkierServlet extends HttpServlet {

  Gson gson = new Gson();

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
//      out.print(gson.toJson(message));
//      out.flush();
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
//      out.write(gson.toJson(message));
//      out.flush();
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
//      out.print(gson.toJson(message));
//      out.flush();
      return;
    }
    String[] urls = urlPath.split("/");

    if (!validURL(urls)) {
      message.setMessage("Invalid Request!");
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      res.getWriter().write(gson.toJson(message));
    } else {
      try {
        StringBuilder sb = new StringBuilder();
        String s;
        while ((s = req.getReader().readLine()) != null) {
          sb.append(s);
        }

        LiftRide liftRide = (LiftRide) gson.fromJson(sb.toString(), LiftRide.class);
        if (liftRide.getLiftID() == null || liftRide.getTime() == null
            || liftRide.getResortId() < 0) {
          message.setMessage("Invalid Request!");
          res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        res.setStatus(HttpServletResponse.SC_OK);
        res.getWriter().write(gson.toJson(message));
//        out.write(gson.toJson(message));
//        out.flush();
      } catch (Exception e) {
        e.printStackTrace();
        message.setMessage("Unsuccessful!");
        out.write(gson.toJson(message));
        out.flush();
      }
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
