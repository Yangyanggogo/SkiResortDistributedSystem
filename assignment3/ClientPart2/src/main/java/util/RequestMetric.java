package util;

public class RequestMetric {
  private final long startTime;
  private final String requestType = "POST";
  private final long latency;
  private final int responseCode;

  public RequestMetric(long startTime, long latency, int responseCode) {
    this.startTime = startTime;
    this.latency = latency;
    this.responseCode = responseCode;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getRequestType() {
    return requestType;
  }

  public long getLatency() {
    return latency;
  }

  public int getResponseCode() {
    return responseCode;
  }

  @Override
  public String toString() {
    // Format as CSV row
    return startTime + "," + requestType + "," + latency + "," + responseCode;
  }
}

