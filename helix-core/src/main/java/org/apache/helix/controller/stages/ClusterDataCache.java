package org.apache.helix.controller.stages;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.caches.AbstractDataCache;
import org.apache.helix.common.caches.CurrentStateCache;
import org.apache.helix.common.caches.InstanceMessagesCache;
import org.apache.helix.common.caches.PropertyCache;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.HelixConstants.*;

/**
 * Reads the data from the cluster using data accessor. This output ClusterData which
 * provides useful methods to search/lookup properties
 */
public class ClusterDataCache {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterDataCache.class.getName());
  private static final List<ChangeType> _noFullRefreshProperty =
      Arrays.asList(ChangeType.EXTERNAL_VIEW, ChangeType.TARGET_EXTERNAL_VIEW);

  private final String _clusterName;
  private String _eventId = AbstractDataCache.UNKNOWN_EVENT_ID;
  private ClusterConfig _clusterConfig;
  private boolean _updateInstanceOfflineTime = true;
  private boolean _isTaskCache;
  private boolean _isMaintenanceModeEnabled;
  private ExecutorService _asyncTasksThreadPool;

  // A map recording what data has changed
  private Map<ChangeType, Boolean> _propertyDataChangedMap;

  // Property caches
  private final PropertyCache<ExternalView> _externalViewCache;
  private final PropertyCache<ExternalView> _targetExternalViewCache;
  private final PropertyCache<ResourceConfig> _resourceConfigCache;
  private final PropertyCache<InstanceConfig> _instanceConfigCache;
  private final PropertyCache<LiveInstance> _liveInstanceCache;
  private final PropertyCache<IdealState> _idealStateCache;
  private final PropertyCache<ClusterConstraints> _clusterConstraintsCache;
  private final PropertyCache<StateModelDefinition> _stateModelDefinitionCache;

  // Special caches
  private CurrentStateCache _currentStateCache;
  private TaskDataCache _taskDataCache;
  private InstanceMessagesCache _instanceMessagesCache;

  // Other miscellaneous caches
  private Map<String, Long> _instanceOfflineTimeMap;
  private Map<String, Map<String, String>> _idealStateRuleMap;
  private Map<String, Map<String, MissingTopStateRecord>> _missingTopStateMap = new HashMap<>();
  private Map<String, Map<String, String>> _lastTopStateLocationMap = new HashMap<>();
  private Map<String, Map<String, Set<String>>> _disabledInstanceForPartitionMap = new HashMap<>();
  private Set<String> _disabledInstanceSet = new HashSet<>();

  // maintain a cache of bestPossible assignment across pipeline runs
  // TODO: this is only for customRebalancer, remove it and merge it with _idealMappingCache.
  private Map<String, ResourceAssignment> _resourceAssignmentCache = new HashMap<>();

  // maintain a cache of idealmapping (preference list) for full-auto resource across pipeline runs
  private Map<String, ZNRecord> _idealMappingCache = new HashMap<>();

  private Map<String, Integer> _participantActiveTaskCount = new HashMap<>();

  // For detecting liveinstance and target resource partition state change in task assignment
  // Used in AbstractTaskDispatcher
  private boolean _existsLiveInstanceOrCurrentStateChange = false;

  // These two flags are used to detect ClusterConfig change or LiveInstance/InstanceConfig change
  private boolean _existsClusterConfigChange = false;
  private boolean _existsInstanceChange = false;

  public ClusterDataCache() {
    this(null);
  }

  public ClusterDataCache(String clusterName) {
    _propertyDataChangedMap = new ConcurrentHashMap<>();
    for (ChangeType type : ChangeType.values()) {
      // refresh every type when it is initialized
      _propertyDataChangedMap.put(type, true);
    }
    _clusterName = clusterName;

    _externalViewCache = new PropertyCache<>(_clusterName, "ExternalView", new PropertyCache.PropertyCacheKeyFuncs<ExternalView>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().externalViews();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().externalView(objName);
      }

      @Override
      public String getObjName(ExternalView obj) {
        return obj.getResourceName();
      }
    }, true);
    _targetExternalViewCache = new PropertyCache<>(_clusterName, "TargetExternalView", new PropertyCache.PropertyCacheKeyFuncs<ExternalView>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().targetExternalViews();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().targetExternalView(objName);
      }

      @Override
      public String getObjName(ExternalView obj) {
        return obj.getResourceName();
      }
    }, true);
    _resourceConfigCache = new PropertyCache<>(_clusterName, "ResourceConfig", new PropertyCache.PropertyCacheKeyFuncs<ResourceConfig>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().resourceConfigs();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().resourceConfig(objName);
      }

      @Override
      public String getObjName(ResourceConfig obj) {
        return obj.getResourceName();
      }
    }, true);
    _liveInstanceCache = new PropertyCache<>(_clusterName, "LiveInstance", new PropertyCache.PropertyCacheKeyFuncs<LiveInstance>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().liveInstances();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().liveInstance(objName);
      }

      @Override
      public String getObjName(LiveInstance obj) {
        return obj.getInstanceName();
      }
    }, true);
    _instanceConfigCache = new PropertyCache<>(_clusterName, "InstanceConfig", new PropertyCache.PropertyCacheKeyFuncs<InstanceConfig>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().instanceConfigs();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().instanceConfig(objName);
      }

      @Override
      public String getObjName(InstanceConfig obj) {
        return obj.getInstanceName();
      }
    }, true);
    _idealStateCache = new PropertyCache<>(_clusterName, "IdealState", new PropertyCache.PropertyCacheKeyFuncs<IdealState>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().idealStates();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().idealStates(objName);
      }

      @Override
      public String getObjName(IdealState obj) {
        return obj.getResourceName();
      }
    }, true);
    _clusterConstraintsCache = new PropertyCache<>(_clusterName, "ClusterConstraint", new PropertyCache.PropertyCacheKeyFuncs<ClusterConstraints>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().constraints();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().constraint(objName);
      }

      @Override
      public String getObjName(ClusterConstraints obj) {
        // We set constraint type to the HelixProperty id
        return obj.getId();
      }
    }, false);
    _stateModelDefinitionCache = new PropertyCache<>(_clusterName, "StateModelDefinition", new PropertyCache.PropertyCacheKeyFuncs<StateModelDefinition>() {
      @Override
      public PropertyKey getRootKey(HelixDataAccessor accessor) {
        return accessor.keyBuilder().stateModelDefs();
      }

      @Override
      public PropertyKey getObjPropertyKey(HelixDataAccessor accessor, String objName) {
        return accessor.keyBuilder().stateModelDef(objName);
      }

      @Override
      public String getObjName(StateModelDefinition obj) {
        return obj.getId();
      }
    }, false);

    _currentStateCache = new CurrentStateCache(_clusterName);
    _taskDataCache = new TaskDataCache(_clusterName);
    _instanceMessagesCache = new InstanceMessagesCache(_clusterName);
  }

  private void updateCachePipelineNames() {
    // TODO (harry): remove such update cache name after we have specific cache for
    // different pipelines
    String pipelineType = _isTaskCache ? "TASK" : "DEFAULT";
    _externalViewCache.setPipelineName(pipelineType);
    _targetExternalViewCache.setPipelineName(pipelineType);
    _resourceConfigCache.setPipelineName(pipelineType);
    _liveInstanceCache.setPipelineName(pipelineType);
    _instanceConfigCache.setPipelineName(pipelineType);
    _idealStateCache.setPipelineName(pipelineType);
    _clusterConstraintsCache.setPipelineName(pipelineType);
    _stateModelDefinitionCache.setPipelineName(pipelineType);
  }

  private void refreshIdealState(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(ChangeType.IDEAL_STATE)) {
      _propertyDataChangedMap.put(ChangeType.IDEAL_STATE, false);
      clearCachedResourceAssignments();
      _idealStateCache.refresh(accessor);
    } else {
      LogUtil.logInfo(LOG, getEventId(), String
          .format("No ideal state change for %s cluster, %s pipeline", _clusterName,
              _idealStateCache.getPipelineName()));
    }
  }

  private void refreshLiveInstances(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(ChangeType.LIVE_INSTANCE)) {
      _propertyDataChangedMap.put(ChangeType.LIVE_INSTANCE, false);
      clearCachedResourceAssignments();
      _liveInstanceCache.refresh(accessor);
      _updateInstanceOfflineTime = true;
    } else {
      LogUtil.logInfo(LOG, getEventId(), String
          .format("No live instance change for %s cluster, %s pipeline", _clusterName,
              _liveInstanceCache.getPipelineName()));
    }
  }

  private void refreshInstanceConfigs(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(ChangeType.INSTANCE_CONFIG)) {
      _existsInstanceChange = true;
      _propertyDataChangedMap.put(ChangeType.INSTANCE_CONFIG, false);
      clearCachedResourceAssignments();
      _instanceConfigCache.refresh(accessor);
      LogUtil.logInfo(LOG, getEventId(), String
          .format("Reloaded InstanceConfig for cluster %s, %s pipeline. Keys: %s", _clusterName,
              _instanceConfigCache.getPipelineName(),
              _instanceConfigCache.getPropertyMap().keySet()));
    } else {
      LogUtil.logInfo(LOG, getEventId(), String
          .format("No instance config change for %s cluster, %s pipeline", _clusterName,
              _liveInstanceCache.getPipelineName()));
    }
  }

  private void refreshResourceConfig(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(ChangeType.RESOURCE_CONFIG)) {
      _propertyDataChangedMap.put(ChangeType.RESOURCE_CONFIG, false);
      clearCachedResourceAssignments();
      _resourceConfigCache.refresh(accessor);
      LogUtil.logInfo(LOG, getEventId(), String
          .format("Reloaded ResourceConfig for cluster %s, %s pipeline. Cnt: %s", _clusterName,
              _resourceConfigCache.getPipelineName(),
              _resourceConfigCache.getPropertyMap().keySet().size()));
    } else {
      LogUtil.logInfo(LOG, getEventId(), String
          .format("No resource config change for %s cluster, %s pipeline", _clusterName,
              _liveInstanceCache.getPipelineName()));
    }
  }

  private void refreshExternalViews(final HelixDataAccessor accessor) {
    // As we are not listening on external view change, external view will be
    // refreshed once during the cache's first refresh() call, or when full refresh is required
    if (_propertyDataChangedMap.get(ChangeType.EXTERNAL_VIEW)) {
      _externalViewCache.refresh(accessor);
      _propertyDataChangedMap.put(ChangeType.EXTERNAL_VIEW, false);
    }
  }

  private void refreshTargetExternalViews(final HelixDataAccessor accessor) {
    if (_propertyDataChangedMap.get(ChangeType.TARGET_EXTERNAL_VIEW)) {
      if (_clusterConfig != null && _clusterConfig.isTargetExternalViewEnabled()) {
        _targetExternalViewCache.refresh(accessor);

        // Only set the change type back once we get refreshed with data accessor for the
        // first time
        _propertyDataChangedMap.put(ChangeType.TARGET_EXTERNAL_VIEW, false);
      }
    }
  }

  private void updateMaintenanceInfo(final HelixDataAccessor accessor) {
    MaintenanceSignal maintenanceSignal = accessor.getProperty(accessor.keyBuilder().maintenance());
    _isMaintenanceModeEnabled = maintenanceSignal != null;
  }

  /**
   * This refreshes the cluster data by re-fetching the data from zookeeper in
   * an efficient way
   * @param accessor
   * @return
   */
  public synchronized boolean refresh(HelixDataAccessor accessor) {
    long startTime = System.currentTimeMillis();

    // Reset the LiveInstance/CurrentState change flag
    _existsLiveInstanceOrCurrentStateChange = false;

    // Refresh raw data
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    refreshIdealState(accessor);
    refreshLiveInstances(accessor);
    refreshInstanceConfigs(accessor);
    refreshResourceConfig(accessor);
    _stateModelDefinitionCache.refresh(accessor);
    _clusterConstraintsCache.refresh(accessor);
    refreshExternalViews(accessor);
    refreshTargetExternalViews(accessor);
    updateMaintenanceInfo(accessor);

    // Refresh derived data
    _instanceMessagesCache.refresh(accessor, _liveInstanceCache.getPropertyMap());
    _currentStateCache.refresh(accessor, _liveInstanceCache.getPropertyMap());

    // current state must be refreshed before refreshing relay messages
    // because we need to use current state to validate all relay messages.
    _instanceMessagesCache.updateRelayMessages(_liveInstanceCache.getPropertyMap(),
        _currentStateCache.getCurrentStatesMap());

    if (_updateInstanceOfflineTime) {
      // TODO: make it async
      updateOfflineInstanceHistory(accessor);
    }

    if (_clusterConfig != null) {
      _idealStateRuleMap = _clusterConfig.getIdealStateRules();
    } else {
      _idealStateRuleMap = new HashMap<>();
      LogUtil.logWarn(LOG, _eventId,
          "Cluster config is null for " + (_isTaskCache ? "TASK" : "DEFAULT") + "pipeline");
    }

    updateDisabledInstances();


    // TaskFramework related operations

    // This is for targeted jobs' task assignment. It needs to watch for current state changes for
    // when targeted resources' state transitions complete
    if (_propertyDataChangedMap.get(ChangeType.CURRENT_STATE)) {
      _existsLiveInstanceOrCurrentStateChange = true;
      _propertyDataChangedMap.put(ChangeType.CURRENT_STATE, false);
    }

    // This is for AssignableInstances. Whenever there is a quota config change in ClusterConfig, we
    // must trigger an update to AssignableInstanceManager
    if (_propertyDataChangedMap.get(ChangeType.CLUSTER_CONFIG)) {
      _existsClusterConfigChange = true;
      _propertyDataChangedMap.put(ChangeType.CLUSTER_CONFIG, false);
    }

    if (_isTaskCache) {
      // Refresh TaskCache
      _taskDataCache.refresh(accessor, _resourceConfigCache.getPropertyMap());

      // Refresh AssignableInstanceManager
      AssignableInstanceManager assignableInstanceManager =
          _taskDataCache.getAssignableInstanceManager();
      // Build from scratch every time
      assignableInstanceManager.buildAssignableInstances(_clusterConfig, _taskDataCache,
          _liveInstanceCache.getPropertyMap(), _instanceConfigCache.getPropertyMap());
      // TODO: (Hunter) Consider this for optimization after fixing the problem of quotas not being

      assignableInstanceManager.logQuotaProfileJSON(false);
    }

    long endTime = System.currentTimeMillis();
    LogUtil.logInfo(LOG, _eventId,
        "END: ClusterDataCache.refresh() for cluster " + getClusterName() + ", took "
            + (endTime - startTime) + " ms for " + (_isTaskCache ? "TASK" : "DEFAULT")
            + "pipeline");

    dumpDebugInfo();
    return true;
  }

  private void dumpDebugInfo() {
    if (LOG.isDebugEnabled()) {
      LogUtil.logDebug(LOG, _eventId,
          "# of StateModelDefinition read from zk: " + _stateModelDefinitionCache.getPropertyMap().size());
      LogUtil.logDebug(LOG, _eventId, "# of ConstraintMap read from zk: " + _clusterConstraintsCache.getPropertyMap().size());
      LogUtil.logDebug(LOG, _eventId,
          "LiveInstances: " + _liveInstanceCache.getPropertyMap().keySet());
      for (LiveInstance instance : _liveInstanceCache.getPropertyMap().values()) {
        LogUtil.logDebug(LOG, _eventId,
            "live instance: " + instance.getInstanceName() + " " + instance.getSessionId());
      }
      LogUtil
          .logDebug(LOG, _eventId, "IdealStates: " + _idealStateCache.getPropertyMap().keySet());
      LogUtil.logDebug(LOG, _eventId,
          "ResourceConfigs: " + _resourceConfigCache.getPropertyMap().keySet());
      LogUtil.logDebug(LOG, _eventId,
          "InstanceConfigs: " + _instanceConfigCache.getPropertyMap().keySet());
      LogUtil.logDebug(LOG, _eventId, "ClusterConfigs: " + _clusterConfig);
      LogUtil.logDebug(LOG, _eventId, "JobContexts: " + _taskDataCache.getContexts().keySet());
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Cache content: " + toString());
    }
  }

  private void updateDisabledInstances() {
    // Move the calculating disabled instances to refresh
    _disabledInstanceForPartitionMap.clear();
    _disabledInstanceSet.clear();
    for (InstanceConfig config : _instanceConfigCache.getPropertyMap().values()) {
      Map<String, List<String>> disabledPartitionMap = config.getDisabledPartitionsMap();
      if (!config.getInstanceEnabled()) {
        _disabledInstanceSet.add(config.getInstanceName());
      }
      for (String resource : disabledPartitionMap.keySet()) {
        if (!_disabledInstanceForPartitionMap.containsKey(resource)) {
          _disabledInstanceForPartitionMap.put(resource, new HashMap<String, Set<String>>());
        }
        for (String partition : disabledPartitionMap.get(resource)) {
          if (!_disabledInstanceForPartitionMap.get(resource).containsKey(partition)) {
            _disabledInstanceForPartitionMap.get(resource).put(partition, new HashSet<String>());
          }
          _disabledInstanceForPartitionMap.get(resource).get(partition)
              .add(config.getInstanceName());
        }
      }
    }
    if (_clusterConfig != null && _clusterConfig.getDisabledInstances() != null) {
      _disabledInstanceSet.addAll(_clusterConfig.getDisabledInstances().keySet());
    }
  }

  private void updateOfflineInstanceHistory(HelixDataAccessor accessor) {
    List<String> offlineNodes =
        new ArrayList<>(_instanceConfigCache.getPropertyMap().keySet());
    offlineNodes.removeAll(_liveInstanceCache.getPropertyMap().keySet());
    _instanceOfflineTimeMap = new HashMap<>();

    for (String instance : offlineNodes) {
      Builder keyBuilder = accessor.keyBuilder();
      PropertyKey propertyKey = keyBuilder.participantHistory(instance);
      ParticipantHistory history = accessor.getProperty(propertyKey);
      if (history == null) {
        history = new ParticipantHistory(instance);
      }
      if (history.getLastOfflineTime() == ParticipantHistory.ONLINE) {
        history.reportOffline();
        // persist history back to ZK.
        if (!accessor.setProperty(propertyKey, history)) {
          LogUtil.logError(LOG, _eventId,
              "Fails to persist participant online history back to ZK!");
        }
      }
      _instanceOfflineTimeMap.put(instance, history.getLastOfflineTime());
    }
    _updateInstanceOfflineTime = false;
  }

  public ClusterConfig getClusterConfig() {
    return _clusterConfig;
  }

  public void setClusterConfig(ClusterConfig clusterConfig) {
    _clusterConfig = clusterConfig;
  }

  public String getClusterName() {
    return _clusterConfig != null ? _clusterConfig.getClusterName() : _clusterName;
  }

  /**
   * Return the last offline time map for all offline instances.
   * @return
   */
  public Map<String, Long> getInstanceOfflineTimeMap() {
    return _instanceOfflineTimeMap;
  }

  /**
   * Retrieves the idealstates for all resources
   * @return
   */
  public Map<String, IdealState> getIdealStates() {
    return _idealStateCache.getPropertyMap();
  }

  public synchronized void setIdealStates(List<IdealState> idealStates) {
    _idealStateCache.setPropertyMap(HelixProperty.convertListToMap(idealStates));
  }

  public Map<String, Map<String, String>> getIdealStateRules() {
    return _idealStateRuleMap;
  }

  /**
   * Returns the LiveInstances for each of the instances that are curretnly up and running
   * @return
   */
  public Map<String, LiveInstance> getLiveInstances() {
    return _liveInstanceCache.getPropertyMap();
  }

  /**
   * Return the set of all instances names.
   */
  public Set<String> getAllInstances() {
    return _instanceConfigCache.getPropertyMap().keySet();
  }

  /**
   * Return all the live nodes that are enabled
   * @return A new set contains live instance name and that are marked enabled
   */
  public Set<String> getEnabledLiveInstances() {
    Set<String> enabledLiveInstances = new HashSet<>(getLiveInstances().keySet());
    enabledLiveInstances.removeAll(getDisabledInstances());

    return enabledLiveInstances;
  }

  /**
   * Return all nodes that are enabled.
   * @return
   */
  public Set<String> getEnabledInstances() {
    Set<String> enabledNodes = new HashSet<>(getAllInstances());
    enabledNodes.removeAll(getDisabledInstances());

    return enabledNodes;
  }

  /**
   * Return all the live nodes that are enabled and tagged with given instanceTag.
   * @param instanceTag The instance group tag.
   * @return A new set contains live instance name and that are marked enabled and have the
   *         specified tag.
   */
  public Set<String> getEnabledLiveInstancesWithTag(String instanceTag) {
    Set<String> enabledLiveInstancesWithTag = new HashSet<>(getLiveInstances().keySet());
    Set<String> instancesWithTag = getInstancesWithTag(instanceTag);
    enabledLiveInstancesWithTag.retainAll(instancesWithTag);
    enabledLiveInstancesWithTag.removeAll(getDisabledInstances());

    return enabledLiveInstancesWithTag;
  }

  /**
   * Return all the nodes that are tagged with given instance tag.
   * @param instanceTag The instance group tag.
   */
  public Set<String> getInstancesWithTag(String instanceTag) {
    Set<String> taggedInstances = new HashSet<>();
    for (String instance : _instanceConfigCache.getPropertyMap().keySet()) {
      InstanceConfig instanceConfig = _instanceConfigCache.getPropertyByName(instance);
      if (instanceConfig != null && instanceConfig.containsTag(instanceTag)) {
        taggedInstances.add(instance);
      }
    }

    return taggedInstances;
  }

  public synchronized void setLiveInstances(List<LiveInstance> liveInstances) {
    _liveInstanceCache.setPropertyMap(HelixProperty.convertListToMap(liveInstances));
    _updateInstanceOfflineTime = true;

    // TODO: Move this when listener for LiveInstance is being refactored
    _existsInstanceChange = true;
    _existsLiveInstanceOrCurrentStateChange = true;
  }

  /**
   * Provides the current state of the node for a given session id, the sessionid can be got from
   * LiveInstance
   * @param instanceName
   * @param clientSessionId
   * @return
   */
  public Map<String, CurrentState> getCurrentState(String instanceName, String clientSessionId) {
    return _currentStateCache.getCurrentState(instanceName, clientSessionId);
  }

  /**
   * Provides a list of current outstanding transitions on a given instance.
   * @param instanceName
   * @return
   */
  public Map<String, Message> getMessages(String instanceName) {
    return _instanceMessagesCache.getMessages(instanceName);
  }

  /**
   * Provides a list of current outstanding pending relay messages on a given instance.
   * @param instanceName
   * @return
   */
  public Map<String, Message> getRelayMessages(String instanceName) {
    return _instanceMessagesCache.getRelayMessages(instanceName);
  }

  public void cacheMessages(Collection<Message> messages) {
    _instanceMessagesCache.cacheMessages(messages);
  }

  /**
   * Provides the state model definition for a given state model
   * @param stateModelDefRef
   * @return
   */
  public StateModelDefinition getStateModelDef(String stateModelDefRef) {
    return _stateModelDefinitionCache.getPropertyByName(stateModelDefRef);
  }

  /**
   * Provides all state model definitions
   * @return state model definition map
   */
  public Map<String, StateModelDefinition> getStateModelDefMap() {
    return _stateModelDefinitionCache.getPropertyMap();
  }

  /**
   * Provides the idealstate for a given resource
   * @param resourceName
   * @return
   */
  public IdealState getIdealState(String resourceName) {
    return _idealStateCache.getPropertyByName(resourceName);
  }

  /**
   * Returns the instance config map
   * @return
   */
  public Map<String, InstanceConfig> getInstanceConfigMap() {
    return _instanceConfigCache.getPropertyMap();
  }

  /**
   * Set the instance config map
   * @param instanceConfigMap
   */
  public void setInstanceConfigMap(Map<String, InstanceConfig> instanceConfigMap) {
    _instanceConfigCache.setPropertyMap(instanceConfigMap);
  }

  /**
   * Returns the resource config map
   * @return
   */
  public Map<String, ResourceConfig> getResourceConfigMap() {
    return _resourceConfigCache.getPropertyMap();
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   */
  public void notifyDataChange(ChangeType changeType) {
    _propertyDataChangedMap.put(changeType, true);
  }

  /**
   * Notify the cache that some part of the cluster data has been changed.
   */
  public void notifyDataChange(ChangeType changeType, String pathChanged) {
    notifyDataChange(changeType);
  }

  public ResourceConfig getResourceConfig(String resource) {
    return _resourceConfigCache.getPropertyByName(resource);
  }

  /**
   * Returns job config map
   * @return
   */
  public Map<String, JobConfig> getJobConfigMap() {
    return _taskDataCache.getJobConfigMap();
  }

  /**
   * Returns job config
   * @param resource
   * @return
   */
  public JobConfig getJobConfig(String resource) {
    return _taskDataCache.getJobConfig(resource);
  }

  /**
   * Returns workflow config map
   * @return
   */
  public Map<String, WorkflowConfig> getWorkflowConfigMap() {
    return _taskDataCache.getWorkflowConfigMap();
  }

  /**
   * Returns workflow config
   * @param resource
   * @return
   */
  public WorkflowConfig getWorkflowConfig(String resource) {
    return _taskDataCache.getWorkflowConfig(resource);
  }

  /**
   * Some partitions might be disabled on specific nodes.
   * This method allows one to fetch the set of nodes where a given partition is disabled
   * @param partition
   * @return
   */
  public Set<String> getDisabledInstancesForPartition(String resource, String partition) {
    Set<String> disabledInstancesForPartition = new HashSet<>(_disabledInstanceSet);
    if (_disabledInstanceForPartitionMap.containsKey(resource)
        && _disabledInstanceForPartitionMap.get(resource).containsKey(partition)) {
      disabledInstancesForPartition
          .addAll(_disabledInstanceForPartitionMap.get(resource).get(partition));
    }

    return disabledInstancesForPartition;
  }

  /**
   * This method allows one to fetch the set of nodes that are disabled
   * @return
   */
  public Set<String> getDisabledInstances() {
    return Collections.unmodifiableSet(_disabledInstanceSet);
  }

  /**
   * Returns the number of replicas for a given resource.
   * @param resourceName
   * @return
   */
  public int getReplicas(String resourceName) {
    int replicas = -1;
    Map<String, IdealState> idealStateMap = _idealStateCache.getPropertyMap();

    if (idealStateMap.containsKey(resourceName)) {
      String replicasStr = idealStateMap.get(resourceName).getReplicas();

      if (replicasStr != null) {
        if (replicasStr.equals(IdealState.IdealStateConstants.ANY_LIVEINSTANCE.toString())) {
          replicas = _liveInstanceCache.getPropertyMap().size();
        } else {
          try {
            replicas = Integer.parseInt(replicasStr);
          } catch (Exception e) {
            LogUtil.logError(LOG, _eventId, "invalid replicas string: " + replicasStr + " for "
                + (_isTaskCache ? "TASK" : "DEFAULT") + "pipeline");
          }
        }
      } else {
        LogUtil.logError(LOG, _eventId, "idealState for resource: " + resourceName
            + " does NOT have replicas for " + (_isTaskCache ? "TASK" : "DEFAULT") + "pipeline");
      }
    }
    return replicas;
  }

  /**
   * Returns the ClusterConstraints for a given constraintType
   * @param type
   * @return
   */
  public ClusterConstraints getConstraint(ConstraintType type) {
    return _clusterConstraintsCache.getPropertyByName(type.name());
  }

  public Map<String, Map<String, MissingTopStateRecord>> getMissingTopStateMap() {
    return _missingTopStateMap;
  }

  public Map<String, Map<String, String>> getLastTopStateLocationMap() {
    return _lastTopStateLocationMap;
  }

  public Integer getParticipantActiveTaskCount(String instance) {
    return _participantActiveTaskCount.get(instance);
  }

  public void setParticipantActiveTaskCount(String instance, int taskCount) {
    _participantActiveTaskCount.put(instance, taskCount);
  }

  /**
   * Reset RUNNING/INIT tasks count in JobRebalancer
   */
  public void resetActiveTaskCount(CurrentStateOutput currentStateOutput) {
    // init participant map
    for (String liveInstance : getLiveInstances().keySet()) {
      _participantActiveTaskCount.put(liveInstance, 0);
    }
    // Active task == init and running tasks
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithPendingState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.INIT.name()),
        _participantActiveTaskCount);
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithPendingState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.RUNNING.name()),
        _participantActiveTaskCount);
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithCurrentState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.INIT.name()),
        _participantActiveTaskCount);
    fillActiveTaskCount(
        currentStateOutput.getPartitionCountWithCurrentState(TaskConstants.STATE_MODEL_NAME,
            TaskPartitionState.RUNNING.name()),
        _participantActiveTaskCount);
  }

  private void fillActiveTaskCount(Map<String, Integer> additionPartitionMap,
      Map<String, Integer> partitionMap) {
    for (String participant : additionPartitionMap.keySet()) {
      partitionMap.put(participant,
          partitionMap.get(participant) + additionPartitionMap.get(participant));
    }
  }

  /**
   * Return the JobContext by resource name
   * @param resourceName
   * @return
   */
  public JobContext getJobContext(String resourceName) {
    return _taskDataCache.getJobContext(resourceName);
  }

  /**
   * Return the WorkflowContext by resource name
   * @param resourceName
   * @return
   */
  public WorkflowContext getWorkflowContext(String resourceName) {
    return _taskDataCache.getWorkflowContext(resourceName);
  }

  /**
   * Update context of the Job
   */
  public void updateJobContext(String resourceName, JobContext jobContext) {
    _taskDataCache.updateJobContext(resourceName, jobContext);
  }

  /**
   * Update context of the Workflow
   */
  public void updateWorkflowContext(String resourceName, WorkflowContext workflowContext) {
    _taskDataCache.updateWorkflowContext(resourceName, workflowContext);
  }

  public TaskDataCache getTaskDataCache() {
    return _taskDataCache;
  }

  /**
   * Return map of WorkflowContexts or JobContexts
   * @return
   */
  public Map<String, ZNRecord> getContexts() {
    return _taskDataCache.getContexts();
  }

  /**
   * Returns AssignableInstanceManager.
   * @return
   */
  public AssignableInstanceManager getAssignableInstanceManager() {
    return _taskDataCache.getAssignableInstanceManager();
  }

  public ExternalView getTargetExternalView(String resourceName) {
    return _targetExternalViewCache.getPropertyByName(resourceName);
  }

  public void updateTargetExternalView(String resourceName, ExternalView targetExternalView) {
    _targetExternalViewCache.setProperty(targetExternalView);
  }

  /**
   * Get local cached external view map
   * @return
   */
  public Map<String, ExternalView> getExternalViews() {
    return _externalViewCache.getPropertyMap();
  }

  /**
   * Update the cached external view map
   * @param externalViews
   */
  public void updateExternalViews(List<ExternalView> externalViews) {
    for (ExternalView ev : externalViews) {
      _externalViewCache.setProperty(ev);
    }
  }

  /**
   * Remove dead external views from map
   * @param resourceNames
   */

  public void removeExternalViews(List<String> resourceNames) {
    for (String resourceName : resourceNames) {
      _externalViewCache.deletePropertyByName(resourceName);
    }
  }

  /**
   * Indicate that a full read should be done on the next refresh
   */
  public synchronized void requireFullRefresh() {
    for (ChangeType type : ChangeType.values()) {
      // We only refresh EV and TEV the very first time the cluster data cache is initialized
      if (!_noFullRefreshProperty.contains(type)) {
        _propertyDataChangedMap.put(type, true);
      }
    }
  }

  /**
   * Get async update thread pool
   * @return
   */
  public ExecutorService getAsyncTasksThreadPool() {
    return _asyncTasksThreadPool;
  }

  /**
   * Get cached resourceAssignment (bestPossible mapping) for a resource
   * @param resource
   * @return
   */
  public ResourceAssignment getCachedResourceAssignment(String resource) {
    return _resourceAssignmentCache.get(resource);
  }

  /**
   * Get cached resourceAssignments
   * @return
   */
  public Map<String, ResourceAssignment> getCachedResourceAssignments() {
    return Collections.unmodifiableMap(_resourceAssignmentCache);
  }

  /**
   * Cache resourceAssignment (bestPossible mapping) for a resource
   * @param resource
   * @return
   */
  public void setCachedResourceAssignment(String resource, ResourceAssignment resourceAssignment) {
    _resourceAssignmentCache.put(resource, resourceAssignment);
  }

  /**
   * Get cached resourceAssignment (ideal mapping) for a resource
   * @param resource
   * @return
   */
  public ZNRecord getCachedIdealMapping(String resource) {
    return _idealMappingCache.get(resource);
  }

  /**
   * Invalid the cached resourceAssignment (ideal mapping) for a resource
   * @param resource
   */
  public void invalidCachedIdealStateMapping(String resource) {
    _idealMappingCache.remove(resource);
  }

  /**
   * Get cached idealmapping
   * @return
   */
  public Map<String, ZNRecord> getCachedIdealMapping() {
    return Collections.unmodifiableMap(_idealMappingCache);
  }

  /**
   * Cache resourceAssignment (ideal mapping) for a resource
   * @param resource
   * @return
   */
  public void setCachedIdealMapping(String resource, ZNRecord mapping) {
    _idealMappingCache.put(resource, mapping);
  }

  public void clearCachedResourceAssignments() {
    _resourceAssignmentCache.clear();
    _idealMappingCache.clear();
  }

  /**
   * Set async update thread pool
   * @param asyncTasksThreadPool
   */
  public void setAsyncTasksThreadPool(ExecutorService asyncTasksThreadPool) {
    _asyncTasksThreadPool = asyncTasksThreadPool;
  }

  /**
   * Set the cache is serving for Task pipeline or not
   * @param taskCache
   */
  public void setTaskCache(boolean taskCache) {
    _isTaskCache = taskCache;
    updateCachePipelineNames();
  }

  /**
   * Get the cache is serving for Task pipeline or not
   * @return
   */
  public boolean isTaskCache() {
    return _isTaskCache;
  }

  public boolean isMaintenanceModeEnabled() {
    return _isMaintenanceModeEnabled;
  }

  public void clearMonitoringRecords() {
    _missingTopStateMap.clear();
    _lastTopStateLocationMap.clear();
  }

  public String getEventId() {
    return _eventId;
  }

  public void setEventId(String eventId) {
    _eventId = eventId;
    _idealStateCache.setEventId(eventId);
    _currentStateCache.setEventId(eventId);
    _taskDataCache.setEventId(eventId);
    _liveInstanceCache.setEventId(eventId);
    _instanceConfigCache.setEventId(eventId);
    _resourceConfigCache.setEventId(eventId);
    _stateModelDefinitionCache.setEventId(eventId);
    _clusterConstraintsCache.setEventId(eventId);
  }

  /**
   * Returns whether there has been LiveInstance or CurrentState change. To be used for
   * task-assigning in AbstractTaskDispatcher.
   * @return
   */
  public boolean getExistsLiveInstanceOrCurrentStateChange() {
    return _existsLiveInstanceOrCurrentStateChange;
  }

  /**
   * toString method to print the entire cluster state
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("liveInstaceMap:" + _liveInstanceCache.getPropertyMap()).append("\n");
    sb.append("idealStateMap:" + _idealStateCache.getPropertyMap()).append("\n");
    sb.append("stateModelDefMap:" + _stateModelDefinitionCache.getPropertyMap()).append("\n");
    sb.append("instanceConfigMap:" + _instanceConfigCache.getPropertyMap()).append("\n");
    sb.append("resourceConfigMap:" + _resourceConfigCache.getPropertyMap()).append("\n");
    sb.append("taskDataCache:" + _taskDataCache).append("\n");
    sb.append("messageCache:" + _instanceMessagesCache).append("\n");
    sb.append("currentStateCache:" + _currentStateCache).append("\n");
    sb.append("clusterConfig:" + _clusterConfig).append("\n");

    return sb.toString();
  }
}
