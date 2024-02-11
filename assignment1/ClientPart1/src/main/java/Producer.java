
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;


public class Producer implements Runnable {
  private static final int SKIER_ID_BEGIN = 1;
  private static final int SKIER_ID_END = 100000;
  private static final int RESORT_ID_BEGIN = 1;
  private static final int RESORT_ID_END = 10;
  private static final int LIFT_ID_BEGIN = 1;
  private static final int LIFT_ID_END = 40;
  private static final String SEASON_ID = "2024";
  private static final String DAY_ID = "1";
  private static final int TIME_BEGIN = 1;
  private static final int TIME_END = 360;
  // Mark for end of producer
  private static final int WRONG_RESORT_ID = 101;
  private BlockingQueue<Event> eventQueue;
  int numberOfFlags;
  int total_post_request;

  public Producer(BlockingQueue<Event> eventQueue, int numberOfFlags, int total_post_request) {
    this.eventQueue = eventQueue;
    this.numberOfFlags = numberOfFlags;
    this.total_post_request = total_post_request;
  }

  @Override
  public void run() {
    int p = 0, q = 0;
    while (p<total_post_request){
      Integer currentLiftId = ThreadLocalRandom.current().nextInt(LIFT_ID_BEGIN, LIFT_ID_END + 1);
      Integer currentResortId = ThreadLocalRandom.current().nextInt(RESORT_ID_BEGIN, RESORT_ID_END + 1);
      Integer currentSkierId = ThreadLocalRandom.current().nextInt(SKIER_ID_BEGIN, SKIER_ID_END + 1);
      Integer currentTime = ThreadLocalRandom.current().nextInt(TIME_BEGIN, TIME_END + 1);
      Event currentEvent = new Event(currentSkierId, currentResortId, currentLiftId, SEASON_ID, DAY_ID, currentTime);
      eventQueue.offer(currentEvent);
      p++;
    }

    while (q<numberOfFlags) {
      eventQueue.offer(new Event(0,WRONG_RESORT_ID,0,SEASON_ID,DAY_ID,0));
      q++;
    }
  }
}
