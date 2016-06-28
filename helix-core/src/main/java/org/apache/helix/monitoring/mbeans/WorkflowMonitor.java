package org.apache.helix.monitoring.mbeans;

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

import java.util.Map;

import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;

public class WorkflowMonitor implements WorkflowMonitorMBean {
  private static final String WORKFLOW_KEY = "Workflow";

  private String _clusterName;
  private String _workflowType;
  private TaskDriver _driver;

  private long _allWorkflowCount;
  private long _successfulWorkflowCount;
  private long _failedWorkflowCount;
  private long _existingWorkflowGauge;
  private long _queuedWorkflowGauge;
  private long _runningWorkflowGauge;


  public WorkflowMonitor(String clusterName, String workflowType, TaskDriver driver) {
    _clusterName = clusterName;
    _workflowType = workflowType;
    _driver = driver;
    _allWorkflowCount = 0L;
    _successfulWorkflowCount = 0L;
    _failedWorkflowCount = 0L;
    _existingWorkflowGauge = 0L;
    _queuedWorkflowGauge = 0L;
    _runningWorkflowGauge = 0L;
  }

  @Override
  public long getAllWorkflowCount() {
    return _allWorkflowCount;
  }

  @Override
  public long getSuccessfulWorkflowCount() {
    return _successfulWorkflowCount;
  }

  @Override
  public long getFailedWorkflowCount() {
    return _failedWorkflowCount;
  }

  @Override
  public long getExistingWorkflowGauge() {
    return _existingWorkflowGauge;
  }

  @Override
  public long getQueuedWorkflowGauge() {
    return _queuedWorkflowGauge;
  }

  @Override
  public long getRunningWorkflowGauge() {
    return _runningWorkflowGauge;
  }

  @Override public String getSensorName() {
    return String.format("%s.%s.%s", _clusterName, WORKFLOW_KEY, _workflowType);
  }

  /**
   * Update workflow with transition state
   * @param from The from state of a workflow, created when it is null
   * @param to The to state of a workflow, cleaned by ZK when it is null
   */
  public void updateWorkflowStats(TaskState from, TaskState to) {
    if (from == null) {
      // From null means a new workflow has been created
      _allWorkflowCount++;
      _queuedWorkflowGauge++;
      _existingWorkflowGauge++;
    } else if (from.equals(TaskState.NOT_STARTED)) {
      // From NOT_STARTED means queued workflow number has been decreased one
      _queuedWorkflowGauge--;
    } else if (from.equals(TaskState.IN_PROGRESS)) {
      // From IN_PROGRESS means running workflow number has been decreased one
      _runningWorkflowGauge--;
    }

    if (to == null) {
      // To null means the job has been cleaned from ZK
      _existingWorkflowGauge--;
    } else if (to.equals(TaskState.IN_PROGRESS)) {
      _runningWorkflowGauge++;
    } else if (to.equals(TaskState.FAILED)) {
      _failedWorkflowCount++;
    } else if (to.equals(TaskState.COMPLETED)) {
      _successfulWorkflowCount++;
    }
  }

  /**
   * Refresh workflow gauge
   */
  public void refreshWorkflowStats() {
    _allWorkflowCount = 0L;
    _failedWorkflowCount = 0L;
    _successfulWorkflowCount = 0L;
    _queuedWorkflowGauge = 0L;
    _runningWorkflowGauge = 0L;

    Map<String, WorkflowConfig> workflowConfigMap = _driver.getWorkflows();
    _existingWorkflowGauge = workflowConfigMap.size();

    for (String workflow : workflowConfigMap.keySet()) {
      WorkflowContext workflowContext = _driver.getWorkflowContext(workflow);

      // Skip the recurrent workflows since they are always queued. User will be confused with
      // this kind of workflows counting in queuedWorkflowGauge.
      if (workflowConfigMap.get(workflow).isRecurring()) {
        continue;
      }
      if (workflowContext == null || workflowContext.getWorkflowState()
          .equals(TaskState.NOT_STARTED)) {
        _queuedWorkflowGauge++;
      } else if (workflowContext.getWorkflowState().equals(TaskState.IN_PROGRESS)) {
        _runningWorkflowGauge++;
      }
    }
  }

}
