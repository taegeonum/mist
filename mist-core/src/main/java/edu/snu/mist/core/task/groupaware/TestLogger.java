/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.task.groupaware;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public final class TestLogger {

  private long queryCreationTime = 0;
  private long queryMergingTime = 0;
  private long deserializationTime = 0;
  private long endToendQueryStartTime = 0;

  @Inject
  private TestLogger() {

  }

  public long getQueryCreationTime() {
    return queryCreationTime;
  }

  public void setQueryCreationTime(final long queryCreationTime) {
    this.queryCreationTime = queryCreationTime;
  }

  public long getQueryMergingTime() {
    return queryMergingTime;
  }

  public void setQueryMergingTime(final long queryMergingTime) {
    this.queryMergingTime = queryMergingTime;
  }

  public long getDeserializationTime() {
    return deserializationTime;
  }

  public void setDeserializationTime(final long deserializationTime) {
    this.deserializationTime = deserializationTime;
  }

  public long getEndToendQueryStartTime() {
    return endToendQueryStartTime;
  }

  public void setEndToendQueryStartTime(final long endToendQueryStartTime) {
    this.endToendQueryStartTime = endToendQueryStartTime;
  }

  public void print() {
    System.out.println("Query creation time (w/o merging): " + TimeUnit.NANOSECONDS.toMillis(queryCreationTime));
    System.out.println("Query merging time: " + TimeUnit.NANOSECONDS.toMillis(queryMergingTime));
    System.out.println("Query start time : " + TimeUnit.NANOSECONDS.toMillis(endToendQueryStartTime));
  }
}
