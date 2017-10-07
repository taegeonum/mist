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
package edu.snu.mist.core.task.eventProcessors;

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.parameters.SubGroupId;
import edu.snu.mist.core.task.ExecutionDags;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.eventProcessors.rebalancer.GroupMerger;
import edu.snu.mist.core.task.globalsched.GlobalSchedNonBlockingEventProcessorFactory;
import edu.snu.mist.core.task.globalsched.Group;
import edu.snu.mist.core.task.globalsched.MetaGroup;
import edu.snu.mist.core.task.globalsched.SubGroup;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;

public final class GroupMergerTest {

  private MetaGroup createMetaGroup() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final QueryStarter queryStarter = mock(QueryStarter.class);
    final QueryRemover queryRemover = mock(QueryRemover.class);
    final ExecutionDags executionDags = mock(ExecutionDags.class);

    injector.bindVolatileInstance(QueryStarter.class, queryStarter);
    injector.bindVolatileInstance(QueryRemover.class, queryRemover);
    injector.bindVolatileInstance(ExecutionDags.class, executionDags);

    return injector.getInstance(MetaGroup.class);
  }
  private Group createGroup(final String id) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, id);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector.getInstance(Group.class);
  }

  private SubGroup createSubGroup(final String id) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(SubGroupId.class, id);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector.getInstance(SubGroup.class);
  }

  /**
   * t1: [0.4, 0.2, 0.3, 0.1, 0.05] (1.05) overloaded.
   * t2: [0.1, 0.1, 0.05, 0.05] (0.3) underloaded.
   *
   * After merging.
   * t1: [0.4, 0.2, 0.3] (0.9)
   * t2: [0.1, 0.1, 0.15, 0.1] (0.45)
   */
  @Test
  public void defaultGroupMergerTest1() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "0");
    jcb.bindImplementation(LoadUpdater.class, TestLoadUpdater.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupAllocationTable groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    final GroupMerger groupMerger = injector.getInstance(GroupMerger.class);
    final LoadUpdater loadUpdater = injector.getInstance(LoadUpdater.class);

    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedNonBlockingEventProcessorFactory.class);
    final List<EventProcessor> eventProcessors = new LinkedList<>();
    for (int i = 0; i < 2; i++) {
      eventProcessors.add(epFactory.newEventProcessor());
      groupAllocationTable.put(eventProcessors.get(i));
    }

    final EventProcessor ep1 = eventProcessors.get(0);
    final EventProcessor ep2 = eventProcessors.get(1);

    final MetaGroup mg1 = createMetaGroup();
    final Group g1 = createGroup("g1");
    mg1.addGroup(g1);
    g1.setLoad(0.4);
    g1.setEventProcessor(ep1);

    final MetaGroup mg2 = createMetaGroup();
    final Group g2 = createGroup("g2");
    mg2.addGroup(g2);
    g2.setLoad(0.2);
    g2.setEventProcessor(ep1);

    final MetaGroup mg3 = createMetaGroup();
    final Group g3 = createGroup("g3");
    mg3.addGroup(g3);
    g3.setLoad(0.3);
    g3.setEventProcessor(ep1);

    final MetaGroup mg4 = createMetaGroup();
    final Group g4 = createGroup("g4");
    mg4.addGroup(g4);
    g4.setLoad(0.1);
    g4.setEventProcessor(ep1);

    final SubGroup sg1 = createSubGroup("sg1");
    sg1.setLoad(0.02);
    g4.addSubGroup(sg1);
    final SubGroup sg2 = createSubGroup("sg2");
    sg2.setLoad(0.04);
    g4.addSubGroup(sg2);
    final SubGroup sg3 = createSubGroup("sg3");
    sg3.setLoad(0.04);
    g4.addSubGroup(sg3);

    final MetaGroup mg5 = createMetaGroup();
    final Group g5 = createGroup("g5");
    mg5.addGroup(g5);
    g5.setLoad(0.05);
    g5.setEventProcessor(ep1);

    final SubGroup sg4 = createSubGroup("sg4");
    sg4.setLoad(0.02);
    g5.addSubGroup(sg4);
    final SubGroup sg5 = createSubGroup("sg5");
    sg5.setLoad(0.03);
    g5.addSubGroup(sg5);

    final MetaGroup mg6 = createMetaGroup();
    final Group g6 = createGroup("g6");
    mg6.addGroup(g6);
    g6.setLoad(0.1);
    g6.setEventProcessor(ep2);

    final MetaGroup mg7 = createMetaGroup();
    final Group g7 = createGroup("g7");
    mg7.addGroup(g7);
    g7.setLoad(0.1);
    g7.setEventProcessor(ep2);

    final Group g44 = createGroup("g4");
    g44.setLoad(0.05);
    mg4.addGroup(g44);
    g44.setEventProcessor(ep2);

    final SubGroup sg6 = createSubGroup("sg6");
    sg6.setLoad(0.05);
    g44.addSubGroup(sg6);

    final Group g55 = createGroup("g5");
    mg5.addGroup(g55);
    g55.setLoad(0.05);
    g55.setEventProcessor(ep2);

    final SubGroup sg7 = createSubGroup("sg7");
    sg7.setLoad(0.05);
    g55.addSubGroup(sg7);

    groupAllocationTable.getValue(ep1).add(g1);
    groupAllocationTable.getValue(ep1).add(g2);
    groupAllocationTable.getValue(ep1).add(g3);
    groupAllocationTable.getValue(ep1).add(g4);
    groupAllocationTable.getValue(ep1).add(g5);

    groupAllocationTable.getValue(ep2).add(g6);
    groupAllocationTable.getValue(ep2).add(g7);
    groupAllocationTable.getValue(ep2).add(g44);
    groupAllocationTable.getValue(ep2).add(g55);


    loadUpdater.update();
    groupMerger.groupMerging();

    Assert.assertEquals(1, mg4.getGroups().size());
    Assert.assertEquals(1, mg5.getGroups().size());

    Assert.assertEquals(4, g44.getSubGroups().size());
    Assert.assertEquals(3, g55.getSubGroups().size());

    Assert.assertEquals(g44, sg1.getGroup());
    Assert.assertEquals(g44, sg2.getGroup());
    Assert.assertEquals(g44, sg3.getGroup());
    Assert.assertEquals(g55, sg4.getGroup());
    Assert.assertEquals(g55, sg5.getGroup());

    Assert.assertEquals(0.9, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(0))), 0.0001);
    Assert.assertEquals(0.45, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(1))), 0.0001);

  }

  /**
   * Calculate the load of groups.
   * @param groups groups
   * @return total load
   */
  private double calculateLoadOfGroups(final Collection<Group> groups) {
    double sum = 0;
    for (final Group group : groups) {
      final double fixedLoad = group.getLoad();
      sum += fixedLoad;
    }
    return sum;
  }

  /**
   * A load updater for test.
   */
  static final class TestLoadUpdater implements LoadUpdater {

    private final GroupAllocationTable groupAllocationTable;

    @Inject
    private TestLoadUpdater(final GroupAllocationTable groupAllocationTable) {
      this.groupAllocationTable = groupAllocationTable;
    }

    @Override
    public void update() {
      for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
        double load = 0.0;
        final Collection<Group> groups = groupAllocationTable.getValue(eventProcessor);
        for (final Group group : groups) {
          load += group.getLoad();
        }
        eventProcessor.setLoad(load);
      }
    }
  }
}
