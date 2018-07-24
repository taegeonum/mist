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
package edu.snu.mist.core.eval;

import edu.snu.mist.core.shared.MQTTNoSharedResource;
import edu.snu.mist.core.shared.MQTTResource;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.codeshare.ClassLoaderProvider;
import edu.snu.mist.core.task.codeshare.NoSharingURLClassLoaderProvider;
import edu.snu.mist.core.task.codeshare.URLClassLoaderProvider;
import edu.snu.mist.core.task.groupaware.GroupAllocationTableModifier;
import edu.snu.mist.core.task.groupaware.GroupAwareQueryManagerImpl;
import edu.snu.mist.core.task.ptq.PTQGroupAllocationTableModifier;
import edu.snu.mist.core.task.ptq.PTQQueryManagerImpl;
import edu.snu.mist.core.task.threadbased.ThreadBasedQueryManagerImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

public final class EvalConfigs {
    private final String executionModel;
    private final boolean codeSharing;
    private final boolean networkSharing;
    private final boolean merging;

    @Inject
    private EvalConfigs(@Parameter(ExecutionModel.class) final String executionModel,
                        @Parameter(CodeSharing.class) final boolean codeSharing,
                        @Parameter(NetworkSharing.class) final boolean networkSharing,
                        @Parameter(Merging.class) final boolean merging) {
        this.executionModel = executionModel;
        this.codeSharing = codeSharing;
        this.networkSharing = networkSharing;
        this.merging = merging;
    }

    private Class<? extends QueryManager> getQueryManagerClass() {
        if (executionModel.equals("mist")) {
            return GroupAwareQueryManagerImpl.class;
        } else if (executionModel.equals("tpq")) {
            return ThreadBasedQueryManagerImpl.class;
        } else if (executionModel.equals("ptq")) {
          return PTQQueryManagerImpl.class;
        } else {
            throw new RuntimeException("Invalid execution model: " + executionModel);
        }
    }

    private Class<? extends ClassLoaderProvider> getClassLoaderProviderClass() {
        if (codeSharing) {
            return URLClassLoaderProvider.class;
        } else {
            return NoSharingURLClassLoaderProvider.class;
        }
    }

    public Configuration getConfig() {
        System.out.println("Execution model: " + executionModel);

        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
        jcb.bindNamedParameter(ExecutionModel.class, executionModel);
        jcb.bindNamedParameter(CodeSharing.class, String.valueOf(codeSharing));
        jcb.bindNamedParameter(NetworkSharing.class, String.valueOf(networkSharing));
        jcb.bindNamedParameter(Merging.class, String.valueOf(merging));

        jcb.bindImplementation(QueryManager.class, getQueryManagerClass());
        jcb.bindImplementation(ClassLoaderProvider.class, getClassLoaderProviderClass());

        if (!networkSharing) {
            jcb.bindImplementation(MQTTResource.class, MQTTNoSharedResource.class);
        }

        if (executionModel.equals("ptq")) {
            jcb.bindImplementation(GroupAllocationTableModifier.class, PTQGroupAllocationTableModifier.class);
        }

        return jcb.build();
    }

    public static CommandLine addCommandLineConf(final CommandLine commandLine) {
        return commandLine
                .registerShortNameOfClass(ExecutionModel.class)
                .registerShortNameOfClass(CodeSharing.class)
                .registerShortNameOfClass(NetworkSharing.class)
                .registerShortNameOfClass(Merging.class);
    }
}
