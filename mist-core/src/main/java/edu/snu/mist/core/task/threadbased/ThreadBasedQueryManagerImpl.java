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
package edu.snu.mist.core.task.threadbased;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistEvent;
import edu.snu.mist.core.MistWatermarkEvent;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.ApplicationInfo;
import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.QueryCheckpoint;
import edu.snu.mist.formats.avro.QueryControlResult;
import io.netty.util.internal.ConcurrentSet;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 */
@SuppressWarnings("unchecked")
public final class ThreadBasedQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(ThreadBasedQueryManagerImpl.class.getName());

  /**
   * Scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * A execution and logical dag generator.
   */
  private final DagGenerator dagGenerator;

  /**
   * Map that has the Operator chain as a key and the thread as a value.
   */
  private final Set<Thread> threads;


  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro dag.
   */
  private final ConfigDagGenerator configDagGenerator;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private ThreadBasedQueryManagerImpl(final DagGenerator dagGenerator,
                                      final ScheduledExecutorServiceWrapper schedulerWrapper,
                                      final QueryInfoStore planStore,
                                      final ConfigDagGenerator configDagGenerator) {
    this.dagGenerator = dagGenerator;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.threads = new ConcurrentSet<>();
    this.configDagGenerator = configDagGenerator;
    LOG.info("Thread based start");
  }


  // Return false if the queue is empty or the previously event processing is not finished.
  private boolean processNextEvent(
      final BlockingQueue<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> queue) throws InterruptedException {
    final Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>> event = queue.take();
      for (final Map.Entry<ExecutionVertex, MISTEdge> entry : event.getValue().entrySet()) {
        process(event.getKey(), entry.getValue().getDirection(), (PhysicalOperator)entry.getKey());
      }
    return true;
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
      } else {
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

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param physicalPlan physical plan of the query
   */
  private void start(final ExecutionDag physicalPlan) {
    final List<PhysicalSource> sources = new LinkedList<>();
    final DAG<ExecutionVertex, MISTEdge> dag = physicalPlan.getDag();
    final Iterator<ExecutionVertex> iterator = GraphUtils.topologicalSort(dag);
    final BlockingQueue<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> eventQueue = new LinkedBlockingQueue<>();

    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = dag.getEdges(source);
          // 3) Sets output emitters
          source.setOutputEmitter(new ThreadBasedSourceOutputEmitter<>(nextOps, eventQueue));
          sources.add(source);
          break;
        }
        case OPERATOR: {
          // 2) Inserts the OperatorChain to OperatorChainManager.
          final PhysicalOperator physicalOp = (PhysicalOperator)executionVertex;
          final Map<ExecutionVertex, MISTEdge> edges =
              dag.getEdges(physicalOp);
          physicalOp.getOperator().setOutputEmitter(new OperatorOutputEmitter(edges));
          break;
        }
        case SINK: {
          break;
        }
        default:
          throw new RuntimeException("Invalid vertex type: " + executionVertex.getType());
      }
    }

    // Create a thread
    final Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            processNextEvent(eventQueue);
          } catch (InterruptedException e) {
            // try again
          }
        }
      }
    });

    threads.add(t);
    t.start();

    // 4) starts to receive input data stream from the sources
    for (final PhysicalSource source : sources) {
      source.start();
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    planStore.close();
    for (final Thread thread : threads) {
      thread.interrupt();
    }
  }

  @Override
  public QueryControlResult create(final AvroDag avroDag) {
    return createQueryWithCheckpoint(avroDag, null);
  }

  @Override
  public QueryControlResult createQueryWithCheckpoint(final AvroDag avroDag, final QueryCheckpoint checkpointedState) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    final String queryId = avroDag.getQueryId();
    queryControlResult.setQueryId(queryId);

    final DAG<ConfigVertex, MISTEdge> configDag = configDagGenerator.generate(avroDag);
    try {
      final ExecutionDag executionDag =
              dagGenerator.generate(configDag, avroDag.getJarPaths());

      // Execute the execution dag
      start(executionDag);

      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(queryId));
      return queryControlResult;

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Query createAndStartQuery(final String queryId,
                                   final ApplicationInfo applicationInfo,
                                   final DAG<ConfigVertex, MISTEdge> configDag)
          throws IOException, InjectionException, ClassNotFoundException {
    return null;
  }

  @Override
  public ApplicationInfo createApplication(final String appId,
                                           final List<String> jarFilePath) throws InjectionException {
    return null;
  }

  @Override
  public Group createGroup(final ApplicationInfo applicationInfo) throws InjectionException {
    return null;
  }

  /**
   * Deletes queries from MIST.
   */
  @Override
  public QueryControlResult delete(final String groupId, final String queryId) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    queryControlResult.setIsSuccess(true);
    queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    return queryControlResult;
  }
}