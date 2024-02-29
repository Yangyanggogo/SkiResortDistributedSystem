import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import util.RequestMetric;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

public class ConsumerSingle implements Runnable {
  private Integer countRequest;
  private String ipAddress;
  private boolean phaseOne;
  private int RETRY = 5;
  private  BlockingQueue<Event> eventQueue;
  AtomicInteger cntSuccessPosts;
  AtomicInteger cntFailPosts;
  CountDownLatch latch;
  private ConcurrentLinkedQueue<RequestMetric> metricsQueue;



  public ConsumerSingle(Integer countRequest, String ipAddress, boolean phaseOne, BlockingQueue<Event> eventQueue,
      AtomicInteger cntSuccessPosts, AtomicInteger cntFailPosts, CountDownLatch latch, ConcurrentLinkedQueue<RequestMetric> metricsQueue) {
    this.countRequest = countRequest;
    this.ipAddress = ipAddress;
    this.phaseOne = phaseOne;
    this.eventQueue = eventQueue;
    this.cntSuccessPosts = cntSuccessPosts;
    this.cntFailPosts = cntFailPosts;
    this.latch = latch;
    this.metricsQueue = metricsQueue;

  }

  /*
  When sending post, in phase one, I need to make sure that each thread handle 1000 requests.
  While in phase two, the number of requests handled by each thread is not required.
   */

  @Override
  public void run() {

    String path = "http://" + this.ipAddress + "/Server_war";
    //String path = "http://" + this.ipAddress;
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
        if (sendPost(skiersApi, cur,metricsQueue)) {
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
        if (sendPost(skiersApi, cur, metricsQueue)) {
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
    if (e.getResortId() > 10){
      return false;
    }
    return true;
  }


  private boolean sendPost(SkiersApi skiersApi, Event event, ConcurrentLinkedQueue<RequestMetric> metricsQueue) {
    int tried = 0;
    boolean success = false;
    long startTime = System.currentTimeMillis(); // Capture start time

    while (tried < RETRY && !success) {
      try {
        ApiResponse<Void> apiResponse = skiersApi.writeNewLiftRideWithHttpInfo(
            new LiftRide().liftID(event.getLiftId()).time(event.getTime()),
            event.getResortId(), event.getSeasonId(), event.getDayId(), event.getSkierId());


        long endTime = System.currentTimeMillis(); // Capture end time
        long latency = endTime - startTime; // Calculate latency
        int responseCode = apiResponse.getStatusCode();

        // Record the metric
        metricsQueue.offer(new RequestMetric(startTime, latency, responseCode));

        if (responseCode == HTTP_OK || responseCode == HTTP_CREATED) {
          success = true;
        } else if (responseCode >= 400) {
          tried++;
        }
      } catch (ApiException exception) {
        tried++;
        long endTime = System.currentTimeMillis(); // Capture end time in case of exception
        long latency = endTime - startTime; // Calculate latency
        metricsQueue.offer(new RequestMetric(startTime, latency, exception.getCode()));
        exception.printStackTrace();
      }
    }
    return success;
  }


}

