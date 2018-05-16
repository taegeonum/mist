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

import edu.snu.mist.core.driver.ApplicationJarInfo;
import edu.snu.mist.core.driver.MistRunningTaskInfo;
import edu.snu.mist.core.driver.MistTaskSubmitInfo;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.TaskRequest;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The default master to driver message server implementation.
 */
public final class DefaultMasterToDriverMessageImpl implements MasterToDriverMessage {

  /**
   * The MistDriver's evaluator requestor.
   */
  private final EvaluatorRequestor requestor;

  /**
   * The shared store for mist task configuration.
   */
  private final MistTaskSubmitInfo taskSubmitInfoStore;

  /**
   * The shared running task info for recovery.
   */
  private final MistRunningTaskInfo runningTaskInfo;

  /**
   * The serializer for Tang configuration.
   */
  private final ConfigurationSerializer configurationSerializer;

  /**
   * The shared store for application jar info.
   */
  private final ApplicationJarInfo applicationJarInfo;

  @Inject
  private DefaultMasterToDriverMessageImpl(
      final MistTaskSubmitInfo taskSubmitInfoStore,
      final MistRunningTaskInfo runningTaskInfo,
      final EvaluatorRequestor requestor,
      final ApplicationJarInfo applicationJarInfo) {
    this.taskSubmitInfoStore = taskSubmitInfoStore;
    this.runningTaskInfo = runningTaskInfo;
    this.requestor = requestor;
    this.configurationSerializer = new AvroConfigurationSerializer();
    this.applicationJarInfo = applicationJarInfo;
  }

  @Override
  public synchronized Void requestNewTask(final TaskRequest taskRequest) throws AvroRemoteException {
    try {
      final Configuration conf =
          configurationSerializer.fromString(taskRequest.getSerializedTaskConfiguration());
      // Store task submit info.
      taskSubmitInfoStore.setTaskConfiguration(conf);
      taskSubmitInfoStore.setNewRatio(taskRequest.getNewRatio());
      taskSubmitInfoStore.setReservedCodeCacheSize(taskRequest.getReservedCodeCacheSize());
      // Submit an evaluator requst for tasks.
      requestor.newRequest()
          .setNumber(taskRequest.getTaskNum())
          .setNumberOfCores(taskRequest.getTaskCpuNum())
          .setMemory(taskRequest.getTaskMemSize())
          .submit();
    } catch (final IOException e) {
      e.printStackTrace();
      throw new AvroRemoteException("Cannot deserialize the task configuration!");
    }
    return null;
  }

  @Override
  public boolean saveJarInfo(final String appId,
                             final List<String> jarPaths) throws AvroRemoteException {
    return this.applicationJarInfo.put(appId, jarPaths);
  }

  @Override
  public Map<String, List<String>> retrieveJarInfo() throws AvroRemoteException {
    final Map<String, List<String>> result = new HashMap<>();
    for (final Map.Entry<String, List<String>> entry : this.applicationJarInfo.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  @Override
  public List<String> retrieveRunningTaskInfo() throws AvroRemoteException {
    return runningTaskInfo.retrieveRunningTaskList();
  }
}
