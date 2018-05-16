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

import edu.snu.mist.core.task.ExecutionDags;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;
import edu.snu.mist.core.task.merging.ConfigExecutionVertexMap;
import edu.snu.mist.core.task.merging.QueryIdConfigDagMap;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This interface represents an application.
 * Network connections, codes are shared within this application.
 * Query merging is also being performed in the same application.
 */
@DefaultImplementation(DefaultApplicationInfoImpl.class)
public interface ApplicationInfo {

  /**
   * Get groups of the application.
   */
  List<Group> getGroups();

  /**
   * Get a random group for this application.
   */
  Group getRandomGroup();

  /**
   * Add a group.
   * This method must be called by SingleWriterThread of GroupAllocationTableModifier.
   * @param group group
   */
  boolean addGroup(Group group);

  /**
   * Remove a group.
   * This method must be called by SingleWriterThread of GroupAllocationTableModifier.
   * @param group group
   */
  void removeGroup(Group group);

  /**
   * The number of groups.
   */
  AtomicInteger numGroups();

  /**
   * Get the application id.
   * @return
   */
  String getApplicationId();

  /**
   * Get a jar file path of the application.
   * @return
   */
  List<String> getJarFilePath();

  /**
   * Get the query starter for this application.
   */
  QueryStarter getQueryStarter();

  /**
   * Get the query remover for this application.
   */
  QueryRemover getQueryRemover();

  /**
   * Get the execution dags for this application.
   */
  ExecutionDags getExecutionDags();

  /**
   * Get the map for query Ids and ConfigDags.
   */
  QueryIdConfigDagMap getQueryIdConfigDagMap();

  /**
   * Get the map for Config Vertices and their corresponding Execution Vertices.
   */
  ConfigExecutionVertexMap getConfigExecutionVertexMap();
}