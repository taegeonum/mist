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
package edu.snu.mist.core.task;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;

import java.util.Iterator;
import java.util.Map;

/**
 * This is an utility class for query starter.
 */
public final class QueryStarterUtils {

  private QueryStarterUtils() {
    // do nothing
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks.
   * @param submittedExecutionDag the dag of the submitted query
   */
  public static void setUpOutputEmitters(final ExecutionDag submittedExecutionDag,
                                         final Query query,
                                         final boolean ptq) {
    final DAG<ExecutionVertex, MISTEdge> dag = submittedExecutionDag.getDag();
    final Iterator<ExecutionVertex> iterator = GraphUtils.topologicalSort(dag);
    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = dag.getEdges(source);
          // Sets output emitters
          if (ptq) {
            source.setOutputEmitter(new PTQSourceOutputEmitter<>(nextOps, query));
          } else {
            source.setOutputEmitter(new NonBlockingQueueSourceOutputEmitter<>(nextOps, query));
          }
          break;
        }
        case OPERATOR: {
          final PhysicalOperator operator = (PhysicalOperator)executionVertex;
          final Map<ExecutionVertex, MISTEdge> edges =
              dag.getEdges(operator);
          // Sets output emitters and operator chain manager for operator.
          operator.getOperator().setOutputEmitter(new OperatorOutputEmitter(edges));
          break;
        }
        case SINK: {
          break;
        }
        default:
          throw new RuntimeException("Invalid vertex type: " + executionVertex.getType());
      }
    }
  }
}
