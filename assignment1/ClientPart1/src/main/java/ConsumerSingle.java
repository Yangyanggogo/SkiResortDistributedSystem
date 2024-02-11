import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

public class ConsumerSingle implements Runnable {
  private Integer countRequest;
  private String ipAddress;
  private boolean phaseOne;
  private int RETRY = 5;
  BlockingQueue<Event> eventQueue;
  AtomicInteger cntSuccessPosts;
  AtomicInteger cntFailPosts;
  CountDownLatch latch;



  public ConsumerSingle(Integer countRequest, String ipAddress, boolean phaseOne, BlockingQueue<Event> eventQueue,
      AtomicInteger cntSuccessPosts, AtomicInteger cntFailPosts, CountDownLatch latch) {
    this.countRequest = countRequest;
    this.ipAddress = ipAddress;
    this.phaseOne = phaseOne;
    this.eventQueue = eventQueue;
    this.cntSuccessPosts = cntSuccessPosts;
    this.cntFailPosts = cntFailPosts;
    this.latch = latch;

  }

  /*
  When sending post, in phase one, I need to make sure that each thread handle 1000 requests.
  While in phase two, the number of requests handled by each thread is not required.
   */

  @Override
  public void run() {

//    String path = "http://" + this.ipAddress + "/Server_war";
    String path = "http://" + this.ipAddress + "/Server_war";
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(path);
    SkiersApi skiersApi = new SkiersApi(apiClient);

    int cntSucc = 0;
    int cntFail = 0;
    if (phaseOne) {
      int start = 0;
      while (start < countRequest) {
        Event cur = null;
        try {
          cur = eventQueue.take();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        if (!checkEvent(cur)) {
          break;
        }
        if (sendPost(skiersApi, cur)) {
          cntSucc += 1;
        } else {
          cntFail += 1;
        }
        start++;

      }
    } else {
      while (true) {
        Event cur = null;
        try {
          cur = eventQueue.take();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        if (!checkEvent(cur)) {
          break;
        }
        if (sendPost(skiersApi, cur)) {
          cntSucc += 1;
        } else {
          cntFail += 1;
        }
      }

    }
    this.cntSuccessPosts.getAndAdd(cntSucc);
    this.cntFailPosts.getAndAdd(cntFail);
    this.latch.countDown();


  }

  private boolean checkEvent(Event e) {
    if (e.getResortId() > 10 ){
      return false;
    }
    return true;
  }


  private boolean sendPost(SkiersApi skiersApi, Event event){
    int tried = 0;
    while (tried < RETRY) {
      try {

        LiftRide curLiftRide = new LiftRide();
//        System.out.println(event);
        curLiftRide.setLiftID(event.getLiftId());
        curLiftRide.setTime(event.getTime());
        ApiResponse<Void> apiResponse = skiersApi.writeNewLiftRideWithHttpInfo(curLiftRide, event.getResortId(), event.getSeasonId(), event.getDayId(), event.getSkierId());
        if (apiResponse.getStatusCode() == HTTP_OK || apiResponse.getStatusCode() == HTTP_CREATED) {

          return true;
        }
        if (apiResponse.getStatusCode() >= 400) {
          tried++;
        }

      } catch (ApiException exception) {
        tried++;
        exception.printStackTrace();
      }
    }
    return false;

  }
}

