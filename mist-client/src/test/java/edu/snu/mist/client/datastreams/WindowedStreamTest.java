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
package edu.snu.mist.client.datastreams;

import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.utils.TestParameters;
import edu.snu.mist.client.utils.UDFTestUtils;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.common.windows.WindowData;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * The test class for WindowedStream and operations on WindowedStream.
 */
public class WindowedStreamTest {

  private MISTQueryBuilder queryBuilder;
  private WindowedStream<Tuple2<String, Integer>> timeWindowedStream;

  @Before
  public void setUp() {
    queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(TestParameters.SUPER_GROUP_ID);

    timeWindowedStream = queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .map(s -> new Tuple2<>(s, 1))
        .window(new TimeWindowInformation(5000, 1000));
  }

  @After
  public void tearDown() {
    queryBuilder = null;
  }


  /**
   * Test for reduceByKeyWindow operation.
   */
  @Test
  public void testReduceByKeyWindowStream() throws InjectionException, IOException {
    final MISTBiFunction<Integer, Integer, Integer> reduceFunc = (x, y) -> x + y;
    final ContinuousStream<Map<String, Integer>> reducedWindowStream =
        timeWindowedStream.reduceByKeyWindow(0, String.class, reduceFunc);

    // Get info
    final Map<String, String> conf = reducedWindowStream.getConfiguration();
    Assert.assertEquals("0", conf.get(ConfKeys.ReduceByKeyOperator.KEY_INDEX.name()));
    Assert.assertEquals(SerializeUtils.serializeToString(reduceFunc),
        conf.get(ConfKeys.ReduceByKeyOperator.MIST_BI_FUNC.name()));

    // Check windowed -> reduce by key
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream,
        reducedWindowStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for binding the udf class of applyStatefulWindow operation.
   */
  @Test
  public void testApplyStatefulWindowStream() throws InjectionException, IOException {
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> func =
        new UDFTestUtils.TestApplyStatefulFunction();
    final ContinuousStream<Integer> applyStatefulWindowStream =
        timeWindowedStream.applyStatefulWindow(new UDFTestUtils.TestApplyStatefulFunction());

    /* Simulate two data inputs on UDF stream */
    final Map<String, String> conf = applyStatefulWindowStream.getConfiguration();
    Assert.assertEquals(SerializeUtils.serializeToString(func),
        conf.get(ConfKeys.OperatorConf.UDF_STRING.name()));

    // Check windowed -> stateful operation applied
    checkEdges(
        queryBuilder.build().getDAG(), 1, timeWindowedStream,
        applyStatefulWindowStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for aggregateWindow operation.
   */
  @Test
  public void testAggregateWindowStream() throws InjectionException, IOException, ClassNotFoundException {
    final MISTFunction<WindowData<Tuple2<String, Integer>>, String> func = new WindowAggregateFunction();
    final ContinuousStream<String> aggregateWindowStream
        = timeWindowedStream.aggregateWindow(func);

    final Map<String, String> conf = aggregateWindowStream.getConfiguration();
    Assert.assertEquals(SerializeUtils.serializeToString(func),
        conf.get(ConfKeys.OperatorConf.UDF_STRING.name()));
    // Check windowed -> aggregated
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream,
        aggregateWindowStream, new MISTEdge(Direction.LEFT));
  }

  static final class WindowAggregateFunction implements MISTFunction<WindowData<Tuple2<String, Integer>>, String> {
    @Inject
    public WindowAggregateFunction() {

    }

    @Override
    public String apply(final WindowData<Tuple2<String, Integer>> windowData) {
      String result = "";
      final Iterator<Tuple2<String, Integer>> itr = windowData.getDataCollection().iterator();
      while (itr.hasNext()) {
        final Tuple2<String, Integer> tuple = itr.next();
        result = result.concat("{" + tuple.get(0) + ", " + tuple.get(1).toString() + "}, ");
      }
      return result + windowData.getStart() + ", " + windowData.getEnd();
    }
  }

  /**
   * Checks the size and direction of the edges from upstream.
   */
  private void checkEdges(final DAG<MISTStream, MISTEdge> dag,
                          final int edgesSize,
                          final MISTStream upStream,
                          final MISTStream downStream,
                          final MISTEdge edgeInfo) {
    final Map<MISTStream, MISTEdge> neighbors = dag.getEdges(upStream);
    Assert.assertEquals(edgesSize, neighbors.size());
    Assert.assertEquals(edgeInfo, neighbors.get(downStream));
  }
}