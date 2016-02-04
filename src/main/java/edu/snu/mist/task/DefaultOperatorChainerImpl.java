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

import edu.snu.mist.task.operators.Operator;

import javax.inject.Inject;

// TODO[MIST-69]: Partitioning physical plans into OperatorChains by chaining operators
final class DefaultOperatorChainerImpl implements OperatorChainer {

  @Inject
  private DefaultOperatorChainerImpl() {

  }

  @Override
  public PhysicalPlan<OperatorChain> chainOperators(final PhysicalPlan<Operator> plan) {
    throw new RuntimeException("DefaultPhysicalToChainedPlanImpl.convertToChainedPlan is not implemented yet");
  }
}