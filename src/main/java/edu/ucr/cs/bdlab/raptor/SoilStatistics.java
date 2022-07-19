/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.raptor;

import scala.Array;

import java.util.Arrays;

/**
 * A class that stores simple statistics to be computed on soil data.
 * <ul>
 *   <li>Min: The minimum value</li>
 *   <li>Max: The maximum value</li>
 *   <li>Average: The average value</li>
 *   <li>Stdev: The standard deviation</li>
 *   <li>Median: The median element</li>
 * </ul>
 */
public class SoilStatistics implements Collector {
  /** The summation of all values represented by this object */
  private double sum = 0.0;

  /** The number of elements represented by this object */
  private int count = 0;

  /** The minimum value represented by this object */
  private double max = Double.NEGATIVE_INFINITY;

  /** The maximum value represented by this object */
  private double min = Double.POSITIVE_INFINITY;

  /** Sum of the value squares */
  private double sum2 = 0.0;

  @Override
  public void setNumBands(int n) {
    assert n == 1: "Soil statistics expect one band";
  }

  @Override
  public Collector collect(int column, int row, int[] value) {
    throw new RuntimeException("Unexpected value");
  }

  @Override
  public Collector collect(int column, int row, float[] value) {
    double v = value[0];
    // Skip improperly encoded fill value
    if (v != -9999) {
      count += 1;
      sum += v;
      sum2 += v * v;
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    return this;
  }

  @Override
  public Collector collect(int column, int row, int width, int height, int[] values) {
    throw new RuntimeException("Unexpected value");
  }

  @Override
  public Collector accumulate(Collector c) {
    SoilStatistics s = (SoilStatistics) c;
    this.count += s.count;
    this.sum += s.sum;
    this.sum2 += s.sum2;
    this.min = Math.min(this.min, s.min);
    this.max = Math.max(this.max, s.max);
    return this;
  }

  @Override
  public int getNumBands() {
    return 1;
  }

  @Override
  public void invalidate() {
    this.count = -1;
  }

  @Override
  public boolean isValid() {
    return this.count >= 0;
  }

  public int getCount() {
    return this.count;
  }

  public double getMin() {
    return this.min;
  }

  public double getMax() {
    return this.max;
  }

  public double getAverage() {
    return this.sum / this.count;
  }

  public double getStdev() {
    return count == 1? 0 : Math.sqrt(Math.abs(sum2 - sum * sum / count)) / (count - 1);
  }

//  public double getMedian() {
//    values = Arrays.copyOf(values, count);
//    Arrays.sort(values);
//    return values[values.length / 2];
//  }
}
