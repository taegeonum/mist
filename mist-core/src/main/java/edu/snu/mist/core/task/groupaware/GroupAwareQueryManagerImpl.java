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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.common.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.common.shared.KafkaSharedResource;
import edu.snu.mist.common.shared.MQTTResource;
import edu.snu.mist.common.shared.NettySharedResource;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.parameters.ApplicationIdentifier;
import edu.snu.mist.core.task.groupaware.parameters.JarFilePath;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 * This has a global ThreadManager that manages event processors.
 * TODO[MIST-618]: Make GroupAwareGlobalSchedQueryManager use NextGroupSelector to schedule the group.
 */
@SuppressWarnings("unchecked")
public final class GroupAwareQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(GroupAwareQueryManagerImpl.class.getName());

  /**
   * Scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * Application map.
   */
  private final ApplicationMap applicationMap;

  /**
   * Event processor manager.
   */
  private final EventProcessorManager eventProcessorManager;

  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro dag.
   */
  private final ConfigDagGenerator configDagGenerator;

  /**
   * A globally shared MQTTSharedResource.
   */
  private final MQTTResource mqttSharedResource;

  /**
   * A globally shared KafkaSharedResource.
   */
  private final KafkaSharedResource kafkaSharedResource;

  /**
   * A globally shared NettySharedResource.
   */
  private final NettySharedResource nettySharedResource;

  private final DagGenerator dagGenerator;

  private final GroupAllocationTableModifier groupAllocationTableModifier;

  /**
   * TODO: This should be generate globally unique numbers.
   */
  private final AtomicLong applicationNum = new AtomicLong(0);

  /**
   * The checkpoint period.
   */
  private final long checkpointPeriod;

  private final AtomicInteger queryNum = new AtomicInteger(0);

  private final TestLogger testLogger;

  private final int expectedQueryNum = 500000;

  private final List<Tuple<String, AvroDag>> queries = new ArrayList<>(expectedQueryNum + 10000);

  private final ExecutorService executorService;

  private final int numThread = 56;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private GroupAwareQueryManagerImpl(final ScheduledExecutorServiceWrapper schedulerWrapper,
                                     final QueryInfoStore planStore,
                                     final EventProcessorManager eventProcessorManager,
                                     final ConfigDagGenerator configDagGenerator,
                                     final MQTTResource mqttSharedResource,
                                     final KafkaSharedResource kafkaSharedResource,
                                     final NettySharedResource nettySharedResource,
                                     final DagGenerator dagGenerator,
                                     final GroupAllocationTableModifier groupAllocationTableModifier,
                                     final ApplicationMap applicationMap,
                                     @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
                                     final TestLogger testLogger) {
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.eventProcessorManager = eventProcessorManager;
    this.configDagGenerator = configDagGenerator;
    this.mqttSharedResource = mqttSharedResource;
    this.kafkaSharedResource = kafkaSharedResource;
    this.nettySharedResource = nettySharedResource;
    this.dagGenerator = dagGenerator;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.applicationMap = applicationMap;
    this.checkpointPeriod = checkpointPeriod;
    this.testLogger = testLogger;
    this.executorService = Executors.newFixedThreadPool(numThread);
  }

  private MemoryUsage getHeapMemoryUse() {
    final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    final MemoryUsage heapMemUsage = memBean.getHeapMemoryUsage();
    return heapMemUsage;
  }

  private void createQuery(final Tuple<String, AvroDag> tuple) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    try {
      // Create the submitted query
      // 1) Saves the avr dag to the PlanStore and
      // converts the avro dag to the logical and execution dag
      planStore.saveAvroDag(tuple);
      final String queryId = tuple.getKey();

      // Update app information
      final String appId = tuple.getValue().getAppId();

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Create Query [aid: {0}, qid: {2}]",
            new Object[]{appId, queryId});
      }

      if (!applicationMap.containsKey(appId)) {
        createApplication(appId, tuple.getValue().getJarPaths());
      }

      final long st = System.nanoTime();

      final ApplicationInfo applicationInfo = applicationMap.get(appId);
      final DAG<ConfigVertex, MISTEdge> configDag = configDagGenerator.generate(tuple.getValue());
      // Waiting for group information being added
      while (applicationInfo.getGroups().isEmpty()) {
        Thread.sleep(100);
      }
      final Query query = createAndStartQuery(queryId, applicationInfo, configDag);

      final long et = System.nanoTime();
      testLogger.getEndToendQueryStartTime().addAndGet(et - st);

      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey()));

//      if (queryNum.incrementAndGet() % 10000 == 0) {
//        final MemoryUsage mem = getHeapMemoryUse();
//        System.out.println("## At " + queryNum + " queries Current mem usage: " + (mem.getUsed() / 1000000) + ", "
//            + ", " + (mem.getUsed() / (double)mem.getMax()));
//        testLogger.print();
//      }

      //return queryControlResult;
    } catch (final Exception e) {
      e.printStackTrace();
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting {0} query: {1}",
          new Object[] {tuple.getKey(), e.toString()});

      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      //return queryControlResult;
    }
  }

  /**
   * Start a submitted query.
   * It converts the avro operator chain dag (query) to the execution dag,
   * and executes the sources in order to receives data streams.
   * Before the queries are executed, it stores the avro  dag into disk.
   * We can regenerate the queries from the stored avro dag.
   * @param tuple a pair of the query id and the avro dag
   * @return submission result
   */
  @Override
  public QueryControlResult create(final Tuple<String, AvroDag> tuple) {
    queries.add(tuple);
    if (queryNum.incrementAndGet() == expectedQueryNum) {

      long st = System.currentTimeMillis();
      final List<Future> futures = new ArrayList<>(numThread);
      for (int i = 0; i < numThread; i++) {
        final int partition = expectedQueryNum / numThread;
        final int startIndex = partition * i;
        final int endIndex = i + 1 == numThread ? expectedQueryNum : partition * (i + 1);

        futures.add(executorService.submit(() -> {
          for (int index = startIndex; index < endIndex; index++) {
            createQuery(queries.get(index));
          }
        }));
      }

      for (final Future future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }

      long et = System.currentTimeMillis();
      System.out.println("Query construction time of " + expectedQueryNum + ": " + (et - st));
    }
    return null;
  }

  @Override
  public Query createAndStartQuery(final String queryId,
                                   final ApplicationInfo applicationInfo,
                                   final DAG<ConfigVertex, MISTEdge> configDag)
      throws ClassNotFoundException, IOException {
    final Query query = new DefaultQueryImpl(queryId);
    groupAllocationTableModifier.addEvent(new WritingEvent(WritingEvent.EventType.QUERY_ADD,
        new Tuple<>(applicationInfo, query)));
    // Start the submitted dag
    applicationInfo.getQueryStarter().start(queryId, query, configDag, applicationInfo.getJarFilePath());
    return query;
  }

  @Override
  public ApplicationInfo createApplication(final String appId, final List<String> paths) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    jcb.bindNamedParameter(ApplicationIdentifier.class, appId);
    // TODO: Submit a single jar instead of list of jars
    jcb.bindNamedParameter(JarFilePath.class, paths.get(0));
    jcb.bindNamedParameter(GroupId.class, appId);
    jcb.bindNamedParameter(PeriodicCheckpointPeriod.class, String.valueOf(checkpointPeriod));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(MQTTResource.class, mqttSharedResource);
    injector.bindVolatileInstance(KafkaSharedResource.class, kafkaSharedResource);
    injector.bindVolatileInstance(NettySharedResource.class, nettySharedResource);
    injector.bindVolatileInstance(QueryInfoStore.class, planStore);
    injector.bindVolatileInstance(TestLogger.class, testLogger);

    final ApplicationInfo applicationInfo = injector.getInstance(ApplicationInfo.class);

    applicationMap.putIfAbsent(appId, applicationInfo);

    final Group group = injector.getInstance(Group.class);
    groupAllocationTableModifier.addEvent(
            new WritingEvent(WritingEvent.EventType.GROUP_ADD, new Tuple<>(applicationInfo, group)));

    return applicationInfo;
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    planStore.close();
    eventProcessorManager.close();
  }

  /**
   * Deletes queries from MIST.
   */
  @Override
  public QueryControlResult delete(final String appId, final String queryId) {
    applicationMap.get(appId).getQueryRemover().deleteQuery(queryId);
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    queryControlResult.setIsSuccess(true);
    queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    return queryControlResult;
  }
}
