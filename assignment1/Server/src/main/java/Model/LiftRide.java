package Model;

public class LiftRide {
  public String liftID;
  public String time;

  public int resortId;
  public int seasonId;
  public int dayId;
  public int skierId;

  public LiftRide(String liftID, String time) {
    this.liftID = liftID;
    this.time = time;
  }

  public String getLiftID() {
    return liftID;
  }

  public String getTime() {
    return time;
  }

  public int getResortId() {
    return resortId;
  }

  public int getSeasonId() {
    return seasonId;
  }

  public int getDayId() {
    return dayId;
  }

  public int getSkierId() {
    return skierId;
  }

  public void setLiftID(String liftID) {
    this.liftID = liftID;
  }

  public void setTime(String time) {
    this.time = time;
  }

  public void setResortId(int resortId) {
    this.resortId = resortId;
  }

  public void setSeasonId(int seasonId) {
    this.seasonId = seasonId;
  }

  public void setDayId(int dayId) {
    this.dayId = dayId;
  }

  public void setSkierId(int skierId) {
    this.skierId = skierId;
  }

  @Override
  public String toString() {
    return "LiftRide{" +
        "liftID='" + liftID + '\'' +
        ", time='" + time + '\'' +
        ", resortId=" + resortId +
        ", seasonId=" + seasonId +
        ", dayId=" + dayId +
        ", skierId=" + skierId +
        '}';
  }
}
