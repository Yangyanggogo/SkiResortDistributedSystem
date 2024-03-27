public class Event {

  /**
   * skierID - between 1 and 100000
   * resortID - between 1 and 10
   * liftID - between 1 and 40
   * seasonID - 2024
   * dayID - 1
   * time - between 1 and 360
   */
  private int skierId;
  private int resortId;
  private int liftId;
  private String SeasonId;
  private String dayId;
  private int time;

  public Event(int skierId, int resortId, int liftId, String seasonId, String dayId, int time) {
    this.skierId = skierId;
    this.resortId = resortId;
    this.liftId = liftId;
    SeasonId = seasonId;
    this.dayId = dayId;
    this.time = time;
  }

  public int getSkierId() {
    return skierId;
  }

  public int getResortId() {
    return resortId;
  }

  public int getLiftId() {
    return liftId;
  }

  public String getSeasonId() {
    return SeasonId;
  }

  public String getDayId() {
    return dayId;
  }

  public int getTime() {
    return time;
  }

  public void setSkierId(int skierId) {
    this.skierId = skierId;
  }

  public void setResortId(int resortId) {
    this.resortId = resortId;
  }

  public void setLiftId(int liftId) {
    this.liftId = liftId;
  }

  public void setSeasonId(String seasonId) {
    SeasonId = seasonId;
  }

  public void setDayId(String dayId) {
    this.dayId = dayId;
  }

  public void setTime(int time) {
    this.time = time;
  }

  @Override
  public String toString() {
    return "Event{" +
        "skierId=" + skierId +
        ", resortId=" + resortId +
        ", liftId=" + liftId +
        ", SeasonId='" + SeasonId + '\'' +
        ", dayId='" + dayId + '\'' +
        ", time=" + time +
        '}';
  }
}
