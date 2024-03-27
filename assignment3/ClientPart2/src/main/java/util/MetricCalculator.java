package util;

import static java.util.Collections.sort;

import java.util.List;

public class MetricCalculator {

  public static double getMean(List<Long> input) {
    double total = 0;
    for (long x : input) {
      total += x;
    }
    return total/input.size();
  }

  public static double getMedian(List<Long> input) {
    sort(input);
    double median;
    if (input.size()%2 == 0) {
      double m1 = input.get(input.size()/2-1);
      double m2 = input.get(input.size()/2);
      median = (m1+m2)/2.0;

    } else {
      median = input.get(input.size()/2-1);
    }
    return median;

  }

  public static double getP99(List<Long> input) {
    sort(input);
    double percentage = 99;
    int n = input.size();
    int index = (int)Math.ceil(percentage*n/100.0) - 1;

    return input.get(index);

  }

}
