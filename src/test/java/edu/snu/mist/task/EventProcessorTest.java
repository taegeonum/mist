/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.task;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.task.operators.BaseOperator;
import junit.framework.Assert;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class EventProcessorTest {

  /**
   * Test whether the processor processes events from multiple queries correctly.
   * This test adds 100 events to 2 queries in PartitionedQueryManager
   * and the event processor processes the events by picking the queries randomly.
   */
  @Test
  public void randomPickProcessTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final PartitionedQueryManager queryManager = injector.getInstance(PartitionedQueryManager.class);
    final StringIdentifierFactory idfac = injector.getInstance(StringIdentifierFactory.class);

    final int numTasks = 1000000;
    final List<Integer> list1 = new LinkedList<>();
    final List<Integer> list2 = new LinkedList<>();
    final List<Integer> result = new LinkedList<>();

    final PartitionedQuery query1 = new DefaultPartitionedQuery();
    query1.insertToHead(new TestOperator(
        idfac.getNewInstance("o1"), idfac.getNewInstance("q1")));
    query1.setOutputEmitter((output) -> list1.add((int)output));
    final PartitionedQuery query2 = new DefaultPartitionedQuery();
    query2.insertToHead(new TestOperator(
        idfac.getNewInstance("o2"), idfac.getNewInstance("q2")));
    query2.setOutputEmitter((output) -> list2.add((int)output));

    for (int i = 0; i < numTasks; i++) {
      final int data = i;
      // Add events to queries
      query1.addNextEvent(data);
      query2.addNextEvent(data);
      result.add(data);
    }

    // Add queries to queryManager
    queryManager.insert(query1);
    queryManager.insert(query2);

    // Create a processor
    final Thread processor = new Thread(new EventProcessor(queryManager));
    processor.start();

    while (!(list1.size() == numTasks && list2.size() == numTasks)) {
      // do nothing until the processor consumes all of the events
      Thread.sleep(1000);
    }

    Assert.assertEquals(result, list1);
    Assert.assertEquals(result, list2);
    processor.interrupt();
  }

  /**
   * When multiple EventProcessors process events from a query concurrently,
   * they should process events one by one and do not process multiple events at a time.
   * @throws InjectionException
   */
  @Test
  public void concurrentProcessTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final PartitionedQueryManager queryManager = injector.getInstance(PartitionedQueryManager.class);
    final StringIdentifierFactory idfac = injector.getInstance(StringIdentifierFactory.class);

    final int numTasks = 1000000;
    final List<Integer> list1 = new LinkedList<>();
    final List<Integer> result = new LinkedList<>();

    final PartitionedQuery query = new DefaultPartitionedQuery();
    query.insertToHead(new TestOperator(
        idfac.getNewInstance("o1"), idfac.getNewInstance("q1")));
    query.setOutputEmitter((output) -> list1.add((int)output));

    for (int i = 0; i < numTasks; i++) {
      final int data = i;
      // Add tasks to queues
      query.addNextEvent(data);
      result.add(data);
    }

    // Add query to queryManager
    queryManager.insert(query);

    // Create three processors
    final Thread processor1 = new Thread(new EventProcessor(queryManager));
    final Thread processor2 = new Thread(new EventProcessor(queryManager));
    final Thread processor3 = new Thread(new EventProcessor(queryManager));
    processor1.start();
    processor2.start();
    processor3.start();

    while (!(list1.size() == numTasks)) {
      // do nothing until consumer thread consumes all of the tasks
      Thread.sleep(1000);
    }

    Assert.assertEquals(result, list1);
    processor1.interrupt();
    processor2.interrupt();
    processor3.interrupt();
  }

  /**
   * Test operator for event processor.
   * It just forwards inputs to outputEmitter.
   */
  class TestOperator extends BaseOperator<Integer, Integer> {
    public TestOperator(final Identifier opId,
                        final Identifier queryId) {
      super(opId, queryId);
    }

    @Override
    public StreamType.OperatorType getOperatorType() {
      return null;
    }

    @Override
    public void handle(final Integer input) {
      outputEmitter.emit(input);
    }
  }
}