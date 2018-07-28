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
package edu.snu.mist.core.task.groupaware.groupassigner;

import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.core.task.groupaware.GroupAllocationTable;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A group assigner that assigns a new group to the event processor that has the minimum load.
 * Among multiple underloaded event processors, it selects an underloaded event processor randomly.
 */
public final class RoundRobinGroupAssignerImpl implements GroupAssigner {

  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  private final AtomicInteger index = new AtomicInteger();

  private final ConcurrentMap<String, AtomicInteger> counterMap = new ConcurrentHashMap<>();
  private final List<EventProcessor> eps;

  @Inject
  private RoundRobinGroupAssignerImpl(final GroupAllocationTable groupAllocationTable) {
    this.groupAllocationTable = groupAllocationTable;
    this.eps = groupAllocationTable.getEventProcessorsNotRunningIsolatedGroup();
  }

  /**
   * Assign the new group to the event processor.
   * @param groupInfo new group
   */
  @Override
  public void assignGroup(final Group groupInfo) {
    if (!counterMap.containsKey(groupInfo.getApplicationInfo().getApplicationId())) {
      counterMap.putIfAbsent(groupInfo.getApplicationInfo().getApplicationId(), new AtomicInteger());
    }
    final AtomicInteger counter = counterMap.get(groupInfo.getApplicationInfo().getApplicationId());

    final EventProcessor ep = eps.get(counter.getAndIncrement() % eps.size());
    groupAllocationTable.getValue(ep).add(groupInfo);
    groupInfo.setEventProcessor(ep);
  }

  @Override
  public void initialize() {
  }
}