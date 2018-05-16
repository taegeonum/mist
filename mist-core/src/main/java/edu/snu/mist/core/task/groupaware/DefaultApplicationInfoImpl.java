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
import edu.snu.mist.core.task.groupaware.parameters.ApplicationIdentifier;
import edu.snu.mist.core.task.groupaware.parameters.JarFilePath;
import edu.snu.mist.core.task.merging.ConfigExecutionVertexMap;
import edu.snu.mist.core.task.merging.QueryIdConfigDagMap;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public final class DefaultApplicationInfoImpl implements ApplicationInfo {

  private static final Logger LOG = Logger.getLogger(DefaultApplicationInfoImpl.class.getName());

  private final List<Group> groups;

  private final AtomicInteger numGroups = new AtomicInteger(0);

  /**
   * The jar file path.
   */
  private final List<String> jarFilePath;

  /**
   * The application identifier.
   */
  private final String appId;

  /**
   * The execution dags for this application.
   */
  private final ExecutionDags executionDags;

  /**
   * A query starter.
   */
  private final QueryStarter queryStarter;

  /**
   * Query remover that deletes queries.
   */
  private final QueryRemover queryRemover;

  /**
   * The map for query Ids and ConfigDags.
   */
  private final QueryIdConfigDagMap queryIdConfigDagMap;

  /**
   * The map for Config Vertices and their corresponding Execution Vertices.
   */
  private final ConfigExecutionVertexMap configExecutionVertexMap;

  @Inject
  private DefaultApplicationInfoImpl(@Parameter(ApplicationIdentifier.class) final String appId,
                                     @Parameter(JarFilePath.class) final String jarFilePath,
                                     final ExecutionDags executionDags,
                                     final QueryStarter queryStarter,
                                     final QueryRemover queryRemover,
                                     final QueryIdConfigDagMap queryIdConfigDagMap,
                                     final ConfigExecutionVertexMap configExecutionVertexMap) {
    this.groups = new LinkedList<>();
    this.jarFilePath = Arrays.asList(jarFilePath);
    this.appId = appId;
    this.executionDags = executionDags;
    this.queryStarter = queryStarter;
    this.queryRemover = queryRemover;
    this.queryIdConfigDagMap = queryIdConfigDagMap;
    this.configExecutionVertexMap = configExecutionVertexMap;
  }

  @Override
  public List<Group> getGroups() {
    return groups;
  }

  @Override
  public Group getRandomGroup() {
    final Random random = new Random();
    return groups.get(random.nextInt(groups.size()));
  }

  @Override
  public boolean addGroup(final Group group) {
    group.setApplicationInfo(this);
    numGroups.incrementAndGet();
    return groups.add(group);
  }

  @Override
  public void removeGroup(final Group group) {
    groups.remove(group);
    numGroups.decrementAndGet();
  }

  @Override
  public AtomicInteger numGroups() {
    return numGroups;
  }

  @Override
  public String getApplicationId() {
    return appId;
  }

  @Override
  public List<String> getJarFilePath() {
    return jarFilePath;
  }

  @Override
  public ExecutionDags getExecutionDags() {
    return executionDags;
  }

  @Override
  public QueryStarter getQueryStarter() {
    return queryStarter;
  }

  @Override
  public QueryRemover getQueryRemover() {
    return queryRemover;
  }

  @Override
  public QueryIdConfigDagMap getQueryIdConfigDagMap() {
    return queryIdConfigDagMap;
  }

  @Override
  public ConfigExecutionVertexMap getConfigExecutionVertexMap() {
    return configExecutionVertexMap;
  }
}