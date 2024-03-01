import static java.util.Collections.max;
import static java.util.Collections.min;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import util.CSVWriter;
import util.MetricCalculator;
import util.RequestMetric;

public class MultiThreadsConsumer {
  private static final int INITIAL_THREADS = 32;
  private static final int SECOND_STAGE_THREAD_COUNT = 800;
  private static Integer PHASE_ONE_REQUESTS = 1000;

  private static final int TOTAL_EVENTS = 200000;
  private static final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
  private static String ipAddress = "34.213.54.44:8080";

  //private static String ipAddress = "cs6650-LB-567700519.us-west-2.elb.amazonaws.com";
  private static AtomicInteger cntSuccessPosts;
  private static AtomicInteger cntFailPosts;
  private static ConcurrentLinkedQueue<RequestMetric> metricsQueue = new ConcurrentLinkedQueue<>();

  public static final String CSV_FILE_PATH = "requestMetric.csv";



  public static void main(String[] args) throws InterruptedException {
    ExecutorService executor = Executors.newCachedThreadPool();
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(SECOND_STAGE_THREAD_COUNT);
    cntSuccessPosts = new AtomicInteger(0);
    cntFailPosts = new AtomicInteger(0);


    System.out.println("-------------------------------------");
    System.out.println("Multi threads consumer test start");
    System.out.println("-------------------------------------");
    long start = System.currentTimeMillis();

    // Start Producer
    new Thread(new Producer(eventQueue, SECOND_STAGE_THREAD_COUNT, TOTAL_EVENTS)).start();

    // Initially submit consumers
    for (int i = 0; i < INITIAL_THREADS; i++) {
      executor.submit(new ConsumerSingle(PHASE_ONE_REQUESTS,ipAddress,true,eventQueue,cntSuccessPosts,cntFailPosts,latch1,metricsQueue));
    }

    latch1.await(); // Wait for 1 thread to finish
    for (int j = 0; j < SECOND_STAGE_THREAD_COUNT; j++) {
      executor.submit(new ConsumerSingle(PHASE_ONE_REQUESTS,ipAddress,false,eventQueue,cntSuccessPosts,cntFailPosts,latch2, metricsQueue));
    }
    latch2.await();
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    long end = System.currentTimeMillis();
    long wallTime = end-start;

    System.out.println("Number of threads in phase 2: "+ SECOND_STAGE_THREAD_COUNT);
    System.out.println("Number of successful requests: "+ cntSuccessPosts.get());
    System.out.println("Number of fail requests: "+ cntFailPosts.get());
    System.out.println("Wall Time: "+wallTime);


    List<Long> latencies = new ArrayList<>();
    for (RequestMetric r: metricsQueue) {
      latencies.add( r.getLatency());
    }

    System.out.println("Mean latency= "+ MetricCalculator.getMean(latencies) + " ms");
    System.out.println("Median latency= "+ MetricCalculator.getMedian(latencies)+ " ms");
    System.out.println("P99 latency= "+ MetricCalculator.getP99(latencies)+ " ms");
    System.out.println("Throughout: "+(int)((cntSuccessPosts.get()+cntFailPosts.get())/(double)(wallTime/1000)) + " requests/second");
    System.out.println("Min latency= "+ min(latencies));
    System.out.println("Max latency= "+ max(latencies));

    //CSVWriter.writeMetricsToCsv(CSV_FILE_PATH, metricsQueue);
    System.out.println("-------------------------------------");
    System.out.println("Multi threads consumer test end");
    System.out.println("-------------------------------------");
  }


}


