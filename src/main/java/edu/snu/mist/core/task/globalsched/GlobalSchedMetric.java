/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.core.task.globalsched;

import javax.inject.Inject;

/**
 * A class which contains global metrics such as the number of events or cpu utilization.
 */
final class GlobalSchedMetric {

  /**
   * The number of all events inside the operator chain queues.
   */
  private long numEvents;

  /**
   * The cpu utilization of the whole system provided by a low-level system monitor.
   */
  private double systemCpuUtil;

  /**
   * The cpu utilization of the JVM process provided by a low-level system monitor.
   */
  private double processCpuUtil;

  @Inject
  private GlobalSchedMetric() {
    this.numEvents = 0;
    this.systemCpuUtil = 0;
    this.processCpuUtil = 0;
  }

  public void setNumEvents(final long numEventsToSet) {
    this.numEvents = numEventsToSet;
  }

  public long getNumEvents() {
    return numEvents;
  }

  public double getSystemCpuUtil() {
    return systemCpuUtil;
  }

  public void setSystemCpuUtil(final double cpuUtil) {
    this.systemCpuUtil = cpuUtil;
  }

  public double getProcessCpuUtil() {
    return processCpuUtil;
  }

  public void setProcessCpuUtil(final double processCpuUtil) {
    this.processCpuUtil = processCpuUtil;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof GlobalSchedMetric)) {
      return false;
    }
    final GlobalSchedMetric groupMetric = (GlobalSchedMetric) o;
    return this.numEvents == groupMetric.getNumEvents() && systemCpuUtil == groupMetric.getSystemCpuUtil()
        && processCpuUtil == groupMetric.processCpuUtil;
  }

  @Override
  public int hashCode() {
    return ((Long) this.numEvents).hashCode() * 31 + ((Double) this.systemCpuUtil).hashCode() * 21 +
        ((Double) this.processCpuUtil).hashCode() * 11;
  }
}