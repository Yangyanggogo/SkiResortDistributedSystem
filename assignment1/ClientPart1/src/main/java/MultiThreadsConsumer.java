import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadsConsumer {
  private static final int INITIAL_THREADS = 32;
  private static Integer PHASE_ONE_REQUESTS = 1000;
  private static final int SECOND_STAGE_THREAD_COUNT = 512;
  private static final int TOTAL_EVENTS = 200000;
  private static final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
  private static String ipAddress = "35.164.255.136:8080";
  private static AtomicInteger cntSuccessPosts;
  private static AtomicInteger cntFailPosts;



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
      executor.submit(new ConsumerSingle(PHASE_ONE_REQUESTS,ipAddress,true,eventQueue,cntSuccessPosts,cntFailPosts,latch1));
    }


    latch1.await(); // Wait for 1 thread to finish
    for (int j = 0; j < SECOND_STAGE_THREAD_COUNT; j++) {
      executor.submit(new ConsumerSingle(PHASE_ONE_REQUESTS,ipAddress,false,eventQueue,cntSuccessPosts,cntFailPosts,latch2));
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
    System.out.println("Throughout: "+(int)((cntSuccessPosts.get()+cntFailPosts.get())/(double)(wallTime/1000)) + " requests/second");

    System.out.println("-------------------------------------");
    System.out.println("Multi threads consumer test end");
    System.out.println("-------------------------------------");
  }


}


