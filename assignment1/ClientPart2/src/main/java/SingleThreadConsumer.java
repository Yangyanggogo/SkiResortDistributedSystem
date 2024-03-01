import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import util.RequestMetric;

public class SingleThreadConsumer {
  private static Integer countRequest = 10000;
  private static BlockingQueue<Event> eventQueue;
  private static AtomicInteger cntSuccessPosts;
  private static AtomicInteger cntFailPosts;
  private static CountDownLatch latch;

  private static String ipAddress = "34.219.198.181:8080";
  private static ConcurrentLinkedQueue<RequestMetric> metricsQueue = new ConcurrentLinkedQueue<>();


  public static void main(String[] args) throws InterruptedException {
    eventQueue = new LinkedBlockingQueue<>();
    cntSuccessPosts = new AtomicInteger(0);
    cntFailPosts = new AtomicInteger(0);


    System.out.println("-------------------------------------");
    System.out.println("Single thread consumer test starts");
    System.out.println("-------------------------------------");
    long start = System.currentTimeMillis();
    Producer producer = new Producer(eventQueue,0,countRequest);

    latch = new CountDownLatch(1);
    Thread producerThread = new Thread(producer);
    producerThread.start();

    ConsumerSingle singleConsumer = new ConsumerSingle(countRequest,ipAddress,true,eventQueue,cntSuccessPosts,cntFailPosts,latch,metricsQueue);
    Thread consumerThread = new Thread(singleConsumer);
    consumerThread.start();
    latch.await();
    long end = System.currentTimeMillis();
    long wallTime = end-start;



    System.out.println("Number of successful requests: "+ cntSuccessPosts.get());
    System.out.println("Number of fail requests: "+ cntFailPosts.get());
    System.out.println("Wall Time: "+wallTime);
    System.out.println("Throughout: "+((double)(wallTime)/(cntSuccessPosts.get()+cntFailPosts.get())) + " ms/request");
    System.out.println("Throughout: "+(int)((cntSuccessPosts.get()+cntFailPosts.get())/(double)(wallTime/1000)) + " requests/second");


    System.out.println("-------------------------------------");
    System.out.println("Single thread consumer test ends");
    System.out.println("-------------------------------------");

  }


}
