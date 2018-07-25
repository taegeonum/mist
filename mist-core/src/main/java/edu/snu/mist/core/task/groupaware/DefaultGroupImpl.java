
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
import edu.snu.mist.core.operators.Operator;
import edu.snu.mist.core.operators.StateHandler;
import edu.snu.mist.core.parameters.GroupId;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.merging.ConfigExecutionVertexMap;
import edu.snu.mist.core.task.merging.QueryIdConfigDagMap;
import edu.snu.mist.formats.avro.GroupCheckpoint;
import edu.snu.mist.formats.avro.QueryCheckpoint;
import edu.snu.mist.formats.avro.StateWithTimestamp;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the default implementation of Group.
 */
final class DefaultGroupImpl implements Group {

  private static final Logger LOG = Logger.getLogger(DefaultGroupImpl.class.getName());

  /**
   * Group status.
   */
  private enum GroupStatus {
    READY,
    PROCESSING,
    ISOLATED,
  }

  private final String groupId;

  private final Queue<Query> activeQueryQueue;

  private final AtomicInteger numActiveSubGroup = new AtomicInteger(0);

  private final AtomicReference<EventProcessor> eventProcessor;

  private double load = 0;

  private final List<Query> queryList = new LinkedList<>();

  private ApplicationInfo applicationInfo;

  private final AtomicReference<GroupStatus> groupStatus = new AtomicReference<>(GroupStatus.READY);

  private final AtomicLong processingTime = new AtomicLong(0);

  /**
   * The number of processed events in the group.
   */
  private final AtomicLong totalProcessingEvent;
  /**
   * The latest moved time.
   */
  private long latestMovedTime;

  /**
   * The ExecutionDags for this group.
   */
  private final ExecutionDags executionDags;


  /**
   * The map for query Ids and ConfigDags.
   */
  private final QueryIdConfigDagMap queryIdConfigDagMap;

  /**
   * The map for Config Vertices and their corresponding Execution Vertices.
   */
  private final ConfigExecutionVertexMap configExecutionVertexMap;

  @Inject
  private DefaultGroupImpl(@Parameter(GroupId.class) final String groupId,
                           final ExecutionDags executionDags,
                           final QueryIdConfigDagMap queryIdConfigDagMap,
                           final ConfigExecutionVertexMap configExecutionVertexMap) {
    this.groupId = groupId;
    this.activeQueryQueue = new ConcurrentLinkedQueue<>();
    this.eventProcessor = new AtomicReference<>(null);
    this.latestMovedTime = System.currentTimeMillis();
    this.totalProcessingEvent = new AtomicLong(0);
    this.executionDags = executionDags;
    this.queryIdConfigDagMap = queryIdConfigDagMap;
    this.configExecutionVertexMap = configExecutionVertexMap;
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void addQuery(final Query query) {
    synchronized (queryList) {
      query.setGroup(this);
      queryList.add(query);
      activeQueryQueue.add(query);

      final int n = numActiveSubGroup.getAndIncrement();

      if (n == 0) {
        eventProcessor.get().addActiveGroup(this);
      }
    }
  }

  @Override
  public List<Query> getQueries() {
    return queryList;
  }

  @Override
  public void insert(final Query query) {
    activeQueryQueue.add(query);
    final int n = numActiveSubGroup.getAndIncrement();
    //System.out.println("Event is added at Group, # group: " + n);

    if (n == 0) {
      eventProcessor.get().addActiveGroup(this);
    }
  }

  @Override
  public void delete(final Query query) {
    //eventProcessor.get().removeActiveGroup(this);
    synchronized (queryList) {
      queryList.remove(query);
    }
  }

  @Override
  public void setEventProcessor(final EventProcessor ep) {
    eventProcessor.set(ep);
  }

  @Override
  public EventProcessor getEventProcessor() {
    return eventProcessor.get();
  }

  @Override
  public ApplicationInfo getApplicationInfo() {
    return applicationInfo;
  }

  @Override
  public void setApplicationInfo(final ApplicationInfo applicationInfo) {
    this.applicationInfo = applicationInfo;
  }

  @Override
  public boolean setProcessingFromReady() {
    return groupStatus.compareAndSet(GroupStatus.READY, GroupStatus.PROCESSING);
  }

  @Override
  public void setReady() {
    groupStatus.set(GroupStatus.READY);
  }

  @Override
  public double getLoad() {
    return load;
  }

  @Override
  public String getGroupId() {
    return groupId;
  }

  @Override
  public AtomicLong getProcessingTime() {
    return processingTime;
  }

  @Override
  public void setLoad(final double l) {
    load = l;
  }

  @Override
  public boolean isActive() {
    return numActiveSubGroup.get() > 0;
  }

  @Override
  public int processAllEvent() {
    return processAllEvent(Long.MAX_VALUE);
  }

  private long elapsedTime(final long startTime) {
    return System.currentTimeMillis() - startTime;
  }

  @Override
  public int processAllEvent(final long timeout) {
    int numProcessedEvent = 0;
    final long startTime = System.currentTimeMillis();

    int remain = numActiveSubGroup.get();

    while (remain > 0) {
      remain = numActiveSubGroup.decrementAndGet();
      final Query query = activeQueryQueue.poll();

      if (query == null) {
        throw new RuntimeException("Query should not be null");
      }

      if (query.setProcessingFromReady()) {

        final int processedEvent = query.processAllEvent();

        if (processedEvent != 0) {
          query.getProcessingEvent().getAndAdd(processedEvent);
          numProcessedEvent += processedEvent;
        }

        query.setReady();
      }

      // Reschedule this group if it still has events to process
      if (elapsedTime(startTime) > timeout) {
        final EventProcessor ep = eventProcessor.get();
        // This could be null when the group merger merges the group
        if (ep != null) {
          ep.addActiveGroup(this);
        }
        break;
      }
    }

    return numProcessedEvent;
  }

  @Override
  public void setLatestMovedTime(final long t) {
    latestMovedTime = t;
  }

  @Override
  public long numberOfRemainingEvents() {
    int sum = 0;
    final Iterator<Query> iterator = activeQueryQueue.iterator();
    while (iterator.hasNext()) {
      final Query query = iterator.next();
      sum += query.numberOfRemainingEvents();
    }
    return sum;
  }

  @Override
  public long getLatestMovedTime() {
    return latestMovedTime;
  }

  @Override
  public boolean isSplited() {
    return applicationInfo.getGroups().size() > 1;
  }

  @Override
  public int size() {
    return queryList.size();
  }

  @Override
  public AtomicLong getProcessingEvent() {
    return totalProcessingEvent;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{gid: ");
    sb.append(groupId);
    sb.append(", load: ");
    sb.append(load);
    sb.append("# subGroups: ");
    sb.append(queryList.size());
    sb.append("}");
    return sb.toString();
  }

  @Override
  public ExecutionDags getExecutionDags() {
    return executionDags;
  }

  @Override
  public QueryIdConfigDagMap getQueryIdConfigDagMap() {
    return queryIdConfigDagMap;
  }

  @Override
  public ConfigExecutionVertexMap getConfigExecutionVertexMap() {
    return configExecutionVertexMap;
  }

  @Override
  public GroupCheckpoint checkpoint() {
    final Map<String, QueryCheckpoint> queryCheckpointMap = new HashMap<>();
    final GroupMinimumLatestWatermarkTimeStamp groupTimestamp = new GroupMinimumLatestWatermarkTimeStamp();

    if (queryList.size() == 0) {
      LOG.log(Level.WARNING, "There are no queries in the queryIdConfigDagMap for checkpointing.");
    }
    for (final Query query : queryList) {
      final String queryId = query.getId();
      LOG.log(Level.INFO, "query with id {0} is being checkpointed", new Object[]{queryId});
      queryCheckpointMap.put(queryId,
              getQueryCheckpoint(queryIdConfigDagMap.get(queryId), groupTimestamp));
    }

    return GroupCheckpoint.newBuilder()
            .setCheckpointTimestamp(groupTimestamp.getValue())
            .setGroupId(groupId)
            .setQueryCheckpointMap(queryCheckpointMap)
            .build();
  }

  /**
   * Get the checkpoint data for the query.
   */
  private QueryCheckpoint getQueryCheckpoint(final DAG<ConfigVertex, MISTEdge> configDag,
                                             final GroupMinimumLatestWatermarkTimeStamp groupTimestamp) {

    // Find the minimum of the available checkpoint timestamps for the group.
    // Replaying will start from this timestamp, if this ConfigDag is used for recovery.
    // This is initiated as Long.MAX_VALUE, as this means that there are no stateful operators within this dag,
    // and therefore requires no replay.
    long latestWatermarkTimestamp = Long.MAX_VALUE;
    for (final ConfigVertex cv : configDag.getVertices()) {
      final ExecutionVertex ev = configExecutionVertexMap.get(cv);
      if (ev.getType() == ExecutionVertex.Type.OPERATOR) {
        final Operator op = ((DefaultPhysicalOperatorImpl) ev).getOperator();
        if (op instanceof StateHandler) {
          final StateHandler stateHandler = (StateHandler) op;
          latestWatermarkTimestamp = stateHandler.getLatestTimestampBeforeCheckpoint();
          groupTimestamp.compareAndSetIfSmaller(latestWatermarkTimestamp);
        }
      }
    }

    final Map<String, StateWithTimestamp> stateWithTimestampMap = new HashMap<>();

    for (final ConfigVertex cv : configDag.getVertices()) {
      final ExecutionVertex ev = configExecutionVertexMap.get(cv);
      Map<String, Object> state = null;
      Long checkpointTimestamp = 0L;
      if (ev.getType() == ExecutionVertex.Type.OPERATOR) {
        final Operator op = ((DefaultPhysicalOperatorImpl) ev).getOperator();
        if (op instanceof StateHandler) {
          final StateHandler stateHandler = (StateHandler) op;
          checkpointTimestamp = stateHandler.getMaxAvailableTimestamp(groupTimestamp.getValue());
          if (checkpointTimestamp == null) {
            state = new HashMap<>();
            checkpointTimestamp = 0L;
          } else {
            state = StateSerializer.serializeStateMap(stateHandler.getOperatorState(checkpointTimestamp));
          }
        }
      }
      stateWithTimestampMap.put(cv.getId(), StateWithTimestamp.newBuilder()
              .setVertexState(state)
              .setCheckpointTimestamp(checkpointTimestamp)
              .build());
    }

    return QueryCheckpoint.newBuilder()
            .setQueryState(stateWithTimestampMap)
            .build();
  }

  /**
   * This class serves as a wrapper for the Long class.
   * Its performance is better than that of an AtomicLong class or volatile long type
   * because there are no needs for synchronization.
   */
  private final class GroupMinimumLatestWatermarkTimeStamp {
    private long timestamp;

    public GroupMinimumLatestWatermarkTimeStamp() {
      this.timestamp = Long.MAX_VALUE;
    }

    public long getValue() {
      return timestamp;
    }

    public void compareAndSetIfSmaller(final long newValue) {
      if (newValue < timestamp) {
        timestamp = newValue;
      }
    }
  }
}