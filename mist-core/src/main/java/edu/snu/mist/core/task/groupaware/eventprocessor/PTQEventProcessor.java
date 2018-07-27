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
package edu.snu.mist.core.task.groupaware.eventprocessor;

import edu.snu.mist.core.MistEvent;
import edu.snu.mist.core.task.SourceOutputEmitter;
import edu.snu.mist.core.task.groupaware.Group;
import org.apache.reef.io.Tuple;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This event processor is pinned to a certain core.
 */
public final class PTQEventProcessor implements EventProcessor {

  private static final Logger LOG = Logger.getLogger(PTQEventProcessor.class.getName());

  /**
   * Variable for checking close or not.
   */
  private final int id;

  /**
   * Thread.
   */
  private final Thread thread;

  private final BlockingQueue<Tuple<SourceOutputEmitter, MistEvent>> eventQueue;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  public PTQEventProcessor(final int id) {
    this.id = id;
    this.eventQueue = new LinkedBlockingQueue<>();
    LOG.info("Start ptq event process");
    this.thread = new Thread(() -> {
      while (!closed.get()) {
        try {
          final Tuple<SourceOutputEmitter, MistEvent> tuple = eventQueue.take();
          tuple.getKey().processEvent(tuple.getValue());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  public void close() throws Exception {
    closed.set(true);
  }

  public void addEvent(final SourceOutputEmitter emitter, final MistEvent event) {
    //LOG.info("Add event to ptq event processor");
    eventQueue.add(new Tuple<>(emitter, event));
  }

  @Override
  public void start() {
    thread.start();
  }

  @Override
  public double getLoad() {
    return 0;
  }

  @Override
  public void setLoad(final double l) {
  }

  @Override
  public void addActiveGroup(final Group group) {
  }

  @Override
  public boolean removeActiveGroup(final Group group) {
    return true;
  }

  @Override
  public RuntimeProcessingInfo getCurrentRuntimeInfo() {
    return null;
  }

  @Override
  public void setRunningIsolatedGroup(final boolean val) {
  }

  @Override
  public boolean isRunningIsolatedGroup() {
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("T");
    sb.append(id);
    return sb.toString();
  }
}