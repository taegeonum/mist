/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.ExecutionVertex;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of ExecutionDagsForMerge that uses concurrent hash map.
 */
final class SrcAndDagHashMap implements SrcAndDagMap<String> {

  private final ConcurrentHashMap<String, DAG<ExecutionVertex, MISTEdge>> map;

  @Inject
  private SrcAndDagHashMap() {
    this.map = new ConcurrentHashMap<>();
  }

  @Override
  public DAG<ExecutionVertex, MISTEdge> get(final String conf) {
    return map.get(conf);
  }

  @Override
  public void put(final String conf, final DAG<ExecutionVertex, MISTEdge> dag) {
    map.put(conf, dag);
  }

  @Override
  public void replace(final String conf, final DAG<ExecutionVertex, MISTEdge> dag) {
    map.replace(conf, dag);
  }

  @Override
  public DAG<ExecutionVertex, MISTEdge> remove(final String conf) {
    return map.remove(conf);
  }

  @Override
  public int size() {
    return map.size();
  }
}