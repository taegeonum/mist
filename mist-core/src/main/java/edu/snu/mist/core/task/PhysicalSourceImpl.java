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

import edu.snu.mist.core.OutputEmitter;
import edu.snu.mist.core.sources.DataGenerator;
import edu.snu.mist.core.sources.EventGenerator;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class represents the implementation of Source interface.
 * @param <T> the type of input data
 */
public final class PhysicalSourceImpl<T> extends BasePhysicalVertex implements PhysicalSource {
  private static final Logger LOG = Logger.getLogger(PhysicalSourceImpl.class.getName());

  /**
   * Data generator that generates data.
   */
  private final DataGenerator<T> dataGenerator;

  /**
   * Event generator that generates watermark.
   */
  private final EventGenerator<T> eventGenerator;

  private final AtomicBoolean started = new AtomicBoolean(false);

  public PhysicalSourceImpl(final String sourceId,
                            final Map<String, String> configuration,
                            final DataGenerator<T> dataGenerator, final EventGenerator<T> eventGenerator) {
    super(sourceId, configuration);
    this.dataGenerator = dataGenerator;
    this.eventGenerator = eventGenerator;
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {

      if (dataGenerator != null && eventGenerator != null) {
        dataGenerator.setEventGenerator(eventGenerator);
        eventGenerator.start();
        dataGenerator.start();
      } else {
        throw new RuntimeException("DataGenerator and EventGenerator should be set in " +
            PhysicalSourceImpl.class.getName());
      }
    } else {
      LOG.log(Level.SEVERE, "Source is already started");
    }
  }

  @Override
  public EventGenerator getEventGenerator() {
    return eventGenerator;
  }

  @Override
  public SourceOutputEmitter getSourceOutputEmitter() {
    return (SourceOutputEmitter)eventGenerator.getOutputEmitter();
  }

  @Override
  public void close() throws Exception {
    dataGenerator.close();
    eventGenerator.close();
  }

  @Override
  public Type getType() {
    return Type.SOURCE;
  }

  @Override
  public String getIdentifier() {
    return id;
  }

  @Override
  public void setOutputEmitter(final OutputEmitter emitter) {
    eventGenerator.setOutputEmitter(emitter);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final PhysicalSourceImpl<T> that = (PhysicalSourceImpl<T>) o;

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
