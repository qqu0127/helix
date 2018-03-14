package org.apache.helix.task;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestJobStateOnCreation extends TaskSynchronizedTestBase {

  private static final String RESOURCE_NAME = "TEST_RESOURCE";

  private ClusterDataCache _cache;
  private IdealState _idealState;
  private Resource _resource;
  private CurrentStateOutput _currStateOutput;

  @BeforeClass
  public void beforeClass() throws Exception {
    _cache = new ClusterDataCache();
    _idealState = new IdealState(RESOURCE_NAME);
    _resource = new Resource(RESOURCE_NAME);
    _currStateOutput = new CurrentStateOutput();
    _participants =  new MockParticipantManager[_numNodes];
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipants();
    createManagers();
  }

  @Test
  public void testJobStateOnCreation() {
    // Get ResourceConfig ready
    String workflowName = "testWorkflow";
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(RESOURCE_NAME).setTargetPartitionStates(Sets.newHashSet("SLAVE","MASTER"))
        .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);
    String jobName = "job" + RESOURCE_NAME;
    builder = builder.addJob(jobName, jobConfigBuilder);
    Workflow workflow = builder.build();
    WorkflowConfig workflowConfig = workflow.getWorkflowConfig();
    JobConfig jobConfig = jobConfigBuilder.build();
    workflowConfig.getRecord().merge(jobConfig.getRecord());

    _cache.getJobConfigMap().put(RESOURCE_NAME, jobConfig);
    _cache.getWorkflowConfigMap().put(RESOURCE_NAME, workflowConfig);

    WorkflowRebalancer workflowRebalancer = new WorkflowRebalancer();
    workflowRebalancer.init(_manager);
    ResourceAssignment resourceAssignment = workflowRebalancer
        .computeBestPossiblePartitionState(_cache, _idealState, _resource, _currStateOutput);

    WorkflowContext workflowContext = _cache.getWorkflowContext(RESOURCE_NAME);
    Map<String, TaskState> jobStates = workflowContext.getJobStates();
    for (String job : jobStates.keySet()) {
      Assert.assertEquals(jobStates.get(job), TaskState.NOT_STARTED);
    }
  }
}