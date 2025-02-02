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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.ObjectName;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.google.common.collect.Lists;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

public class ResourceMonitor extends DynamicMBeanProvider {

  public enum RebalanceStatus {
    UNKNOWN,
    NORMAL,
    BEST_POSSIBLE_STATE_CAL_FAILED,
    INTERMEDIATE_STATE_CAL_FAILED
  }

  private static final String GAUGE_METRIC_SUFFIX = "Gauge";

  // Gauges
  private SimpleDynamicMetric<Long> _numOfPartitions;
  private SimpleDynamicMetric<Long> _missingTopStatePartitionsBeyondThresholdGauge;
  private SimpleDynamicMetric<Long> _numOfPartitionsInExternalView;
  private SimpleDynamicMetric<Long> _numOfErrorPartitions;
  private SimpleDynamicMetric<Long> _numNonTopStatePartitions;
  private SimpleDynamicMetric<Long> _externalViewIdealStateDiff;
  private SimpleDynamicMetric<Long> _numLessMinActiveReplicaPartitions;
  private SimpleDynamicMetric<Long> _numLessReplicaPartitions;
  private SimpleDynamicMetric<Long> _numPendingRecoveryRebalanceReplicas;
  private SimpleDynamicMetric<Long> _numPendingLoadRebalanceReplicas;
  private SimpleDynamicMetric<Long> _numRecoveryRebalanceThrottledReplicas;
  private SimpleDynamicMetric<Long> _numLoadRebalanceThrottledReplicas;
  private SimpleDynamicMetric<Long> _numPendingStateTransitions;
  private SimpleDynamicMetric<Long> _rebalanceThrottledByErrorPartitionGauge;

  // Counters
  private SimpleDynamicMetric<Long> _successfulTopStateHandoffDurationCounter;
  private SimpleDynamicMetric<Long> _successTopStateHandoffCounter;

  // A new Gauage _missingTopStatePartitionsBeyondThresholdGauge for reporting number of partitions with missing top
  // state has been added. Reason of deprecating this two metrics is because they are similar and doesn't tell about
  // how many partitions are missing beyond threshold. The average duration of different partitions hands-off would not
  // be helpful and can be used to find that out. Please find more info at https://github.com/apache/helix/pull/2381
  @Deprecated
  private SimpleDynamicMetric<Long> _failedTopStateHandoffCounter;
  @Deprecated
  private HistogramDynamicMetric _partitionTopStateNonGracefulHandoffDurationGauge;

  private SimpleDynamicMetric<Long> _maxSinglePartitionTopStateHandoffDuration;
  @Deprecated
  private SimpleDynamicMetric<Long> _totalMessageReceived; // This should be counter since the value behavior is ever-increasing
  private SimpleDynamicMetric<Long> _totalMessageReceivedCounter;
  // Histograms
  private HistogramDynamicMetric _partitionTopStateHandoffDurationGauge;
  private HistogramDynamicMetric _partitionTopStateHandoffHelixLatencyGauge;

  private SimpleDynamicMetric<String> _rebalanceState;

  private String _tag = ClusterStatusMonitor.DEFAULT_TAG;
  private long _lastResetTime;
  private final String _resourceName;
  private final String _clusterName;
  private final ObjectName _initObjectName;

  // A map of dynamic capacity Gauges. The map's keys could change.
  private final Map<String, SimpleDynamicMetric<Long>> _dynamicCapacityMetricsMap;

  @Override
  public DynamicMBeanProvider register() throws JMException {
    doRegister(buildAttributeList(), _initObjectName);

    return this;
  }

  public enum MonitorState {
    TOP_STATE
  }

  @SuppressWarnings("unchecked")
  public ResourceMonitor(String clusterName, String resourceName, ObjectName objectName)
      throws JMException {
    _clusterName = clusterName;
    _resourceName = resourceName;
    _initObjectName = objectName;
    _dynamicCapacityMetricsMap = new ConcurrentHashMap<>();

    _externalViewIdealStateDiff = new SimpleDynamicMetric("DifferenceWithIdealStateGauge", 0L);
    _numPendingRecoveryRebalanceReplicas =
        new SimpleDynamicMetric("PendingRecoveryRebalanceReplicaGauge", 0L);
    _numLoadRebalanceThrottledReplicas =
        new SimpleDynamicMetric("LoadRebalanceThrottledReplicaGauge", 0L);
    _numRecoveryRebalanceThrottledReplicas =
        new SimpleDynamicMetric("RecoveryRebalanceThrottledReplicaGauge", 0L);
    _numPendingLoadRebalanceReplicas =
        new SimpleDynamicMetric("PendingLoadRebalanceReplicaGauge", 0L);
    _numPendingRecoveryRebalanceReplicas =
        new SimpleDynamicMetric("PendingRecoveryRebalanceReplicaGauge", 0L);
    _numLessReplicaPartitions = new SimpleDynamicMetric("MissingReplicaPartitionGauge", 0L);
    _numLessMinActiveReplicaPartitions =
        new SimpleDynamicMetric("MissingMinActiveReplicaPartitionGauge", 0L);
    _numNonTopStatePartitions = new SimpleDynamicMetric("MissingTopStatePartitionGauge", 0L);
    _missingTopStatePartitionsBeyondThresholdGauge = new SimpleDynamicMetric("MissingTopStatePartitionsBeyondThresholdGauge", 0L);
    _numOfErrorPartitions = new SimpleDynamicMetric("ErrorPartitionGauge", 0L);
    _numOfPartitionsInExternalView = new SimpleDynamicMetric("ExternalViewPartitionGauge", 0L);
    _numOfPartitions = new SimpleDynamicMetric("PartitionGauge", 0L);
    _numPendingStateTransitions = new SimpleDynamicMetric("PendingStateTransitionGauge", 0L);
    _rebalanceThrottledByErrorPartitionGauge =
        new SimpleDynamicMetric("RebalanceThrottledByErrorPartitionGauge", 0L);

    _partitionTopStateHandoffDurationGauge =
        new HistogramDynamicMetric("PartitionTopStateHandoffDurationGauge", new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));

    _partitionTopStateHandoffHelixLatencyGauge =
        new HistogramDynamicMetric("PartitionTopStateHandoffHelixLatencyGauge", new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    _partitionTopStateNonGracefulHandoffDurationGauge =
        new HistogramDynamicMetric("PartitionTopStateNonGracefulHandoffGauge", new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));

    _totalMessageReceived = new SimpleDynamicMetric("TotalMessageReceived", 0L);
    _totalMessageReceivedCounter = new SimpleDynamicMetric("TotalMessageReceivedCounter", 0L);
    _maxSinglePartitionTopStateHandoffDuration =
        new SimpleDynamicMetric("MaxSinglePartitionTopStateHandoffDurationGauge", 0L);
    _failedTopStateHandoffCounter = new SimpleDynamicMetric("FailedTopStateHandoffCounter", 0L);
    _successTopStateHandoffCounter = new SimpleDynamicMetric("SucceededTopStateHandoffCounter", 0L);
    _successfulTopStateHandoffDurationCounter =
        new SimpleDynamicMetric("SuccessfulTopStateHandoffDurationCounter", 0L);

    _rebalanceState = new SimpleDynamicMetric<>("RebalanceStatus", RebalanceStatus.UNKNOWN.name());
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s.%s", ClusterStatusMonitor.RESOURCE_STATUS_KEY, _clusterName,
        _tag, _resourceName);
  }

  public long getPartitionGauge() {
    return _numOfPartitions.getValue();
  }

  public long getErrorPartitionGauge() {
    return _numOfErrorPartitions.getValue();
  }

  public long getMissingTopStatePartitionGauge() {
    return _numNonTopStatePartitions.getValue();
  }

  public long getMissingTopStatePartitionsBeyondThresholdGuage() {
    return _missingTopStatePartitionsBeyondThresholdGauge.getValue();
  }

  public long getDifferenceWithIdealStateGauge() {
    return _externalViewIdealStateDiff.getValue();
  }

  public long getSuccessfulTopStateHandoffDurationCounter() {
    return _successfulTopStateHandoffDurationCounter.getValue();
  }

  public long getSucceededTopStateHandoffCounter() {
    return _successTopStateHandoffCounter.getValue();
  }

  public long getMaxSinglePartitionTopStateHandoffDurationGauge() {
    return _maxSinglePartitionTopStateHandoffDuration.getValue();
  }

  public HistogramDynamicMetric getPartitionTopStateHandoffDurationGauge() {
    return _partitionTopStateHandoffDurationGauge;
  }

  @Deprecated
  public HistogramDynamicMetric getPartitionTopStateNonGracefulHandoffDurationGauge() {
    return _partitionTopStateNonGracefulHandoffDurationGauge;
  }

  public HistogramDynamicMetric getPartitionTopStateHandoffHelixLatencyGauge() {
    return _partitionTopStateHandoffHelixLatencyGauge;
  }

  @Deprecated
  public long getFailedTopStateHandoffCounter() {
    return _failedTopStateHandoffCounter.getValue();
  }

  public long getTotalMessageReceived() {
    return _totalMessageReceived.getValue();
  }

  @Deprecated
  public synchronized void increaseMessageCount(long messageReceived) {
    _totalMessageReceived.updateValue(_totalMessageReceived.getValue() + messageReceived);
  }

  public synchronized void increaseMessageCountWithCounter(long messageReceived) {
    _totalMessageReceivedCounter.updateValue(_totalMessageReceivedCounter.getValue() + messageReceived);
  }

  public String getResourceName() {
    return _resourceName;
  }

  public String getBeanName() {
    return _clusterName + " " + _resourceName;
  }

  public void updateResourceState(ExternalView externalView, IdealState idealState,
      StateModelDefinition stateModelDef) {
    if (externalView == null) {
      _logger.warn("External view is null");
      return;
    }

    String topState = null;
    if (stateModelDef != null) {
      List<String> priorityList = stateModelDef.getStatesPriorityList();
      if (!priorityList.isEmpty()) {
        topState = priorityList.get(0);
      }
    }

    resetResourceStateGauges();

    if (idealState == null) {
      _logger.warn("ideal state is null for {}", _resourceName);
      return;
    }

    assert (_resourceName.equals(idealState.getId()));
    assert (_resourceName.equals(externalView.getId()));

    long numOfErrorPartitions = 0;
    long numOfDiff = 0;
    long numOfPartitionWithTopState = 0;

    Set<String> partitions = idealState.getPartitionSet();
    int replica;
    try {
      replica = Integer.valueOf(idealState.getReplicas());
    } catch (NumberFormatException e) {
      _logger.info("Unspecified replica count for {}, skip updating the ResourceMonitor Mbean: {}", _resourceName,
          idealState.getReplicas());
      return;
    } catch (Exception ex) {
      _logger.warn("Failed to get replica count for {}, cannot update the ResourceMonitor Mbean.", _resourceName);
      return;
    }

    int minActiveReplica = idealState.getMinActiveReplicas();
    minActiveReplica = (minActiveReplica >= 0) ? minActiveReplica : replica;

    Set<String> activeStates = new HashSet<>(stateModelDef.getStatesPriorityList());
    activeStates.remove(stateModelDef.getInitialState());
    activeStates.remove(HelixDefinedState.DROPPED.name());
    activeStates.remove(HelixDefinedState.ERROR.name());

    for (String partition : partitions) {
      Map<String, String> idealRecord = idealState.getInstanceStateMap(partition);
      Map<String, String> externalViewRecord = externalView.getStateMap(partition);

      if (idealRecord == null) {
        idealRecord = Collections.emptyMap();
      }
      if (externalViewRecord == null) {
        externalViewRecord = Collections.emptyMap();
      }
      if (!idealRecord.entrySet().equals(externalViewRecord.entrySet())) {
        numOfDiff++;
      }

      int activeReplicaCount = 0;
      boolean hasTopState = false;
      for (String host : externalViewRecord.keySet()) {
        String currentState = externalViewRecord.get(host);
        if (HelixDefinedState.ERROR.toString().equalsIgnoreCase(currentState)) {
          numOfErrorPartitions++;
        }
        if (topState != null && topState.equalsIgnoreCase(currentState)) {
          hasTopState = true;
        }
        if (currentState != null && activeStates.contains(currentState)) {
          activeReplicaCount++;
        }
      }
      if (hasTopState) {
        numOfPartitionWithTopState ++;
      }
      if (replica > 0 && activeReplicaCount < replica) {
        _numLessReplicaPartitions.updateValue(_numLessReplicaPartitions.getValue() + 1);
      }
      if (minActiveReplica >= 0 && activeReplicaCount < minActiveReplica) {
        _numLessMinActiveReplicaPartitions
            .updateValue(_numLessMinActiveReplicaPartitions.getValue() + 1);
      }
    }

    // Update resource-level metrics
    _numOfPartitions.updateValue((long) partitions.size());
    _numOfErrorPartitions.updateValue(numOfErrorPartitions);
    _externalViewIdealStateDiff.updateValue(numOfDiff);
    _numOfPartitionsInExternalView.updateValue((long) externalView.getPartitionSet().size());
    _numNonTopStatePartitions.updateValue(_numOfPartitions.getValue() - numOfPartitionWithTopState);

    String tag = idealState.getInstanceGroupTag();
    if (tag == null || tag.equals("") || tag.equals("null")) {
      _tag = ClusterStatusMonitor.DEFAULT_TAG;
    } else {
      _tag = tag;
    }
  }

  private void resetResourceStateGauges() {
    _numOfErrorPartitions.updateValue(0L);
    _numNonTopStatePartitions.updateValue(0L);
    _externalViewIdealStateDiff.updateValue(0L);
    _numOfPartitionsInExternalView.updateValue(0L);
    _numLessMinActiveReplicaPartitions.updateValue(0L);
    _numLessReplicaPartitions.updateValue(0L);
  }

  public void updatePendingStateTransitionMessages(int messageCount) {
    _numPendingStateTransitions.updateValue((long) messageCount);
  }

  public void updateStateHandoffStats(MonitorState monitorState, long totalDuration,
      long helixLatency, boolean isGraceful, boolean succeeded) {
    switch (monitorState) {
    case TOP_STATE:
      if (succeeded) {
        _successTopStateHandoffCounter.updateValue(_successTopStateHandoffCounter.getValue() + 1);
        _successfulTopStateHandoffDurationCounter
            .updateValue(_successfulTopStateHandoffDurationCounter.getValue() + totalDuration);
        if (isGraceful) {
          _partitionTopStateHandoffDurationGauge.updateValue(totalDuration);
          _partitionTopStateHandoffHelixLatencyGauge.updateValue(helixLatency);
        } else {
          // TODO: Deprecated. use MissingTopStateBeyondThresholdGuage to find out number of partitions with missing top
          //  state.
          _partitionTopStateNonGracefulHandoffDurationGauge.updateValue(totalDuration);
        }
        if (totalDuration > _maxSinglePartitionTopStateHandoffDuration.getValue()) {
          _maxSinglePartitionTopStateHandoffDuration.updateValue(totalDuration);
          _lastResetTime = System.currentTimeMillis();
        }
      } else {
        // TODO: Deprecated. use MissingTopStateBeyondThresholdGuage to find out number of partitions with missing top
        //  state.
        _failedTopStateHandoffCounter.updateValue(_failedTopStateHandoffCounter.getValue() + 1);
        _missingTopStatePartitionsBeyondThresholdGauge
            .updateValue(_missingTopStatePartitionsBeyondThresholdGauge.getValue() + 1);
        _lastResetTime = System.currentTimeMillis();
      }
      break;
    default:
      _logger.warn(
          String.format("Wrong monitor state \"%s\" that not supported ", monitorState.name()));
    }
  }

  public void updateRebalancerStats(long numPendingRecoveryRebalancePartitions,
      long numPendingLoadRebalancePartitions, long numRecoveryRebalanceThrottledPartitions,
      long numLoadRebalanceThrottledPartitions, boolean rebalanceThrottledByErrorPartitions) {
    _numPendingRecoveryRebalanceReplicas.updateValue(numPendingRecoveryRebalancePartitions);
    _numPendingLoadRebalanceReplicas.updateValue(numPendingLoadRebalancePartitions);
    _numRecoveryRebalanceThrottledReplicas.updateValue(numRecoveryRebalanceThrottledPartitions);
    _numLoadRebalanceThrottledReplicas.updateValue(numLoadRebalanceThrottledPartitions);
    _rebalanceThrottledByErrorPartitionGauge.updateValue(rebalanceThrottledByErrorPartitions? 1L : 0L);
  }

  /**
   * Updates partition weight metric. If the partition capacity keys are changed, all MBean
   * attributes will be updated accordingly: old capacity keys will be replaced with new capacity
   * keys in MBean server.
   *
   * @param partitionWeightMap A map of partition weight: capacity key -> partition weight
   */
  void updatePartitionWeightStats(Map<String, Integer> partitionWeightMap) {
    synchronized (_dynamicCapacityMetricsMap) {
      if (_dynamicCapacityMetricsMap.keySet().equals(partitionWeightMap.keySet())) {
        for (Map.Entry<String, Integer> entry : partitionWeightMap.entrySet()) {
          _dynamicCapacityMetricsMap.get(entry.getKey()).updateValue((long) entry.getValue());
        }
        return;
      }

      // Capacity keys are changed, so capacity attribute map needs to be updated.
      _dynamicCapacityMetricsMap.clear();
      for (Map.Entry<String, Integer> entry : partitionWeightMap.entrySet()) {
        String capacityKey = entry.getKey();
        _dynamicCapacityMetricsMap.put(capacityKey,
            new SimpleDynamicMetric<>(capacityKey + GAUGE_METRIC_SUFFIX, (long) entry.getValue()));
      }
    }

    // Update all MBean attributes.
    updateAttributesInfo(buildAttributeList(),
        "Resource monitor for resource: " + getResourceName());
  }

  public void setRebalanceState(RebalanceStatus state) {
    _rebalanceState.updateValue(state.name());
  }

  public long getExternalViewPartitionGauge() {
    return _numOfPartitionsInExternalView.getValue();
  }

  public long getMissingMinActiveReplicaPartitionGauge() {
    return _numLessMinActiveReplicaPartitions.getValue();
  }

  public long getMissingReplicaPartitionGauge() {
    return _numLessReplicaPartitions.getValue();
  }

  public long getNumPendingRecoveryRebalanceReplicas() {
    return _numPendingRecoveryRebalanceReplicas.getValue();
  }

  public long getNumPendingLoadRebalanceReplicas() {
    return _numPendingLoadRebalanceReplicas.getValue();
  }

  public long getNumRecoveryRebalanceThrottledReplicas() {
    return _numRecoveryRebalanceThrottledReplicas.getValue();
  }

  public long getNumLoadRebalanceThrottledReplicas() {
    return _numLoadRebalanceThrottledReplicas.getValue();
  }

  public long getNumPendingStateTransitionGauge() {
    return _numPendingStateTransitions.getValue();
  }

  public String getRebalanceState() {
    return _rebalanceState.getValue();
  }

  public long getRebalanceThrottledByErrorPartitionGauge() {
    return _rebalanceThrottledByErrorPartitionGauge.getValue();
  }

  public void resetMaxTopStateHandoffGauge() {
    if (_lastResetTime + DEFAULT_RESET_INTERVAL_MS <= System.currentTimeMillis()) {
      _maxSinglePartitionTopStateHandoffDuration.updateValue(0L);
      _missingTopStatePartitionsBeyondThresholdGauge.updateValue(0L);
      _lastResetTime = System.currentTimeMillis();
    }
  }

  private List<DynamicMetric<?, ?>> buildAttributeList() {
    List<DynamicMetric<?, ?>> attributeList = Lists.newArrayList(
        _numOfPartitions,
        _numOfPartitionsInExternalView,
        _numOfErrorPartitions,
        _numNonTopStatePartitions,
        _missingTopStatePartitionsBeyondThresholdGauge,
        _numLessMinActiveReplicaPartitions,
        _numLessReplicaPartitions,
        _numPendingRecoveryRebalanceReplicas,
        _numPendingLoadRebalanceReplicas,
        _numRecoveryRebalanceThrottledReplicas,
        _numLoadRebalanceThrottledReplicas,
        _externalViewIdealStateDiff,
        _successfulTopStateHandoffDurationCounter,
        _successTopStateHandoffCounter,
        _failedTopStateHandoffCounter,
        _maxSinglePartitionTopStateHandoffDuration,
        _partitionTopStateHandoffDurationGauge,
        _partitionTopStateHandoffHelixLatencyGauge,
        _partitionTopStateNonGracefulHandoffDurationGauge,
        _totalMessageReceived,
        _totalMessageReceivedCounter,
        _numPendingStateTransitions,
        _rebalanceState,
        _rebalanceThrottledByErrorPartitionGauge);

    attributeList.addAll(_dynamicCapacityMetricsMap.values());

    return attributeList;
  }
}
