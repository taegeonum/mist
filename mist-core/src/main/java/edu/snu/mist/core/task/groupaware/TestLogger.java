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
import java.util.concurrent.atomic.AtomicLong;

public final class TestLogger {

  private final AtomicLong queryCreationTime = new AtomicLong(0);
  private final AtomicLong queryMergingTime = new AtomicLong(0);
  private final AtomicLong deserializationTime = new AtomicLong(0);
  private final AtomicLong endToendQueryStartTime = new AtomicLong(0);

  @Inject
  private TestLogger() {

  }

  public AtomicLong getQueryCreationTime() {
    return queryCreationTime;
  }

  public AtomicLong getQueryMergingTime() {
    return queryMergingTime;
  }

  public AtomicLong getDeserializationTime() {
    return deserializationTime;
  }

  public AtomicLong getEndToendQueryStartTime() {
    return endToendQueryStartTime;
  }

  public void print() {
    System.out.println("Query (de)serialization time: " + deserializationTime.get());
    System.out.println("Query creation time (w/o merging): " + TimeUnit.NANOSECONDS.toMillis(queryCreationTime.get()));
    System.out.println("Query merging time: " + TimeUnit.NANOSECONDS.toMillis(queryMergingTime.get()));
    System.out.println("Query start time : " + TimeUnit.NANOSECONDS.toMillis(endToendQueryStartTime.get()));
  }
}
