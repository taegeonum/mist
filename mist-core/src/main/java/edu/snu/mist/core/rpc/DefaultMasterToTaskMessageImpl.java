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

import edu.snu.mist.core.task.recovery.RecoveryManager;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;

/**
 * The default master-to-task message implementation.
 */
public final class DefaultMasterToTaskMessageImpl implements MasterToTaskMessage {

  /**
   * The recovery manager.
   */
  private final RecoveryManager recoveryManager;

  @Inject
  private DefaultMasterToTaskMessageImpl(final RecoveryManager recoveryManager) {
    this.recoveryManager = recoveryManager;
  }

  @Override
  public Void startTaskSideRecovery() throws AvroRemoteException {
    recoveryManager.startRecovery();
    return null;
  }
}