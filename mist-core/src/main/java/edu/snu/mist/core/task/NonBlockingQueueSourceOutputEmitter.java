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
package edu.snu.mist.core.task;

import edu.snu.mist.core.MistCheckpointEvent;
import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistEvent;
import edu.snu.mist.core.MistWatermarkEvent;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This emitter enqueues events to the source event queue.
 *  @param <I>
 */
public final class NonBlockingQueueSourceOutputEmitter<I> implements SourceOutputEmitter {

  /**
   * A queue for events.
   */
  private final Queue<MistEvent> queue;

  /**
   * Next operators.
   */
  private final Map<ExecutionVertex, MISTEdge> nextOperators;

  /**
   * Number of events.
   */
  private final AtomicInteger numEvents;

  /**
   * Query that contains this source.
   */
  private final Query query;

  private final AtomicBoolean scheduled = new AtomicBoolean(false);

  public NonBlockingQueueSourceOutputEmitter(final Map<ExecutionVertex, MISTEdge> nextOperators,
                                             final Query query) {
    this.queue = new ConcurrentLinkedQueue<>();
    this.nextOperators = nextOperators;
    this.query = query;
    this.numEvents = new AtomicInteger();
  }

  @Override
  public int processAllEvent() {
    final int n = numEvents.get();
    scheduled.set(false);

    int numProcessedEvent = 0;

    while (numProcessedEvent < n) {
      final MistEvent event = queue.poll();
      if (event == null) {
        break;
      }

      for (final Map.Entry<ExecutionVertex, MISTEdge> entry : nextOperators.entrySet()) {
        process(event, entry.getValue().getDirection(), (PhysicalOperator)entry.getKey());
      }
      numProcessedEvent += 1;
    }

    final int remain = numEvents.addAndGet(-numProcessedEvent);
    if (remain > 0 && !scheduled.get()) {
      if (scheduled.compareAndSet(false, true)) {
        query.insert(this);
      }
    }

    return numProcessedEvent;
  }

  private void process(final MistEvent event,
                       final Direction direction,
                       final PhysicalOperator operator) {
    try {
      if (event.isData()) {
        if (direction == Direction.LEFT) {
          operator.getOperator().processLeftData((MistDataEvent) event);
        } else {
          operator.getOperator().processRightData((MistDataEvent) event);
        }
      } else if (event.isCheckpoint()) {
        if (direction == Direction.LEFT) {
          operator.getOperator().processLeftCheckpoint((MistCheckpointEvent) event);
        } else {
          operator.getOperator().processRightCheckpoint((MistCheckpointEvent) event);
        }
      } else  {
        if (direction == Direction.LEFT) {
          operator.getOperator().processLeftWatermark((MistWatermarkEvent) event);
        } else {
          operator.getOperator().processRightWatermark((MistWatermarkEvent) event);
        }
      }
    } catch (final NullPointerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int numberOfEvents() {
    return numEvents.get();
  }

  @Override
  public Query getQuery() {
    return query;
  }


  @Override
  public void emitData(final MistDataEvent data) {
    try {
      //System.out.println("Event is added at sourceOutputEmitter: " + data.getValue() + ", # events: " + n);
      queue.add(data);
      final int n = numEvents.getAndIncrement();

      if (!scheduled.get()) {
        if (scheduled.compareAndSet(false, true)) {
          query.insert(this);
        }
      }

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitData(final MistDataEvent data, final int index) {
    try {
      // source output emitter does not emit data according to the index
      //System.out.println("Event is added at sourceOutputEmitter: " + data.getValue() + ", # events: " + n);
      queue.add(data);
      final int n = numEvents.getAndIncrement();

      if (!scheduled.get()) {
        if (scheduled.compareAndSet(false, true)) {
          query.insert(this);
        }
      }

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    try {
      queue.add(watermark);
      final int n = numEvents.getAndIncrement();

      if (!scheduled.get()) {
        if (scheduled.compareAndSet(false, true)) {
          query.insert(this);
        }
      }

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitCheckpoint(final MistCheckpointEvent checkpoint) {
    try {
      queue.add(checkpoint);
      final int n = numEvents.getAndIncrement();

      if (!scheduled.get()) {
        if (scheduled.compareAndSet(false, true)) {
          query.insert(this);
        }
      }

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}