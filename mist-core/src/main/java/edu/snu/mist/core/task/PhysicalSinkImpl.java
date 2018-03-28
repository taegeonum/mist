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

import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.core.sinks.Sink;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is an implementation of PhysicalSink.
 */
public final class PhysicalSinkImpl<I> extends BasePhysicalVertex implements PhysicalSink<I> {

  private final Sink<I> sink;

  private static final AtomicInteger SINK_COUNTER = new AtomicInteger(0);

  public PhysicalSinkImpl(final String sinkId,
                          final Map<String, String> configuration,
                          final Sink<I> sink) {
    super(sinkId, configuration);
    System.out.println("Sink counter: " + SINK_COUNTER.incrementAndGet() + ", topic: "
        + configuration.get(ConfKeys.MqttSink.MQTT_SINK_TOPIC));
    this.sink = sink;
  }

  public Sink<I> getSink() {
    return sink;
  }

  @Override
  public Type getType() {
    return Type.SINK;
  }

  @Override
  public String getIdentifier() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final PhysicalSinkImpl<I> that = (PhysicalSinkImpl<I>) o;

    if (!id.equals(that.id)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
