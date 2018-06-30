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
package edu.snu.mist.core.rpc;

import edu.snu.mist.core.master.ProxyToTaskMap;
import edu.snu.mist.core.master.TaskAddressInfoMap;
import edu.snu.mist.core.master.TaskRequestor;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.master.recovery.RecoveryStarter;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The default driver-to-message implementation.
 */
public final class DefaultDriverToMasterMessageImpl implements DriverToMasterMessage {

  /**
   * The task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The task address info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The proxy to task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The task requestor for MistMaster.
   */
  private final TaskRequestor taskRequestor;

  /**
   * The recovery scheduler.
   */
  private final RecoveryScheduler recoveryScheduler;

  /**
   * The single threaded executor service for recovery synchronization.
   */
  private final ExecutorService singleThreadedExecutor;

  @Inject
  private DefaultDriverToMasterMessageImpl(
      final TaskStatsMap taskStatsMap,
      final TaskAddressInfoMap taskAddressInfoMap,
      final ProxyToTaskMap proxyToTaskMap,
      final RecoveryScheduler recoveryScheduler,
      final TaskRequestor taskRequestor) {
    this.taskStatsMap = taskStatsMap;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.recoveryScheduler = recoveryScheduler;
    this.taskRequestor = taskRequestor;
    this.singleThreadedExecutor = Executors.newSingleThreadExecutor();
  }

  @Override
  public Void notifyTaskAllocated(final String allocatedTaskId) throws AvroRemoteException {
    taskRequestor.notifyAllocatedTask();
    return null;
  }

  @Override
  public synchronized Void notifyFailedTask(final String taskId) {
    // Remove the failed task...
    final TaskStats taskStats = taskStatsMap.removeTask(taskId);
    taskAddressInfoMap.remove(taskId);
    proxyToTaskMap.remove(taskId);
    singleThreadedExecutor.submit(new RecoveryStarter(taskStats.getGroupStatsMap(), recoveryScheduler, taskRequestor));
    return null;
  }

}