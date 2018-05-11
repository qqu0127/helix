package org.apache.helix.integration.controller;

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

import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.MockZkHelixDataAccessor;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterDataCacheSelectiveUpdate extends ZkStandAloneCMTestBase {

  @Test()
  public void testUpdateOnNotification() throws Exception {
    MockZkHelixDataAccessor accessor =
        new MockZkHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    ClusterDataCache cache =
        new ClusterDataCache("CLUSTER_" + TestHelper.getTestClassName());
    cache.refresh(accessor);

    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 1);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), NODE_NR + 1);

    accessor.clearReadCounters();

    // refresh again should read nothing
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    // cluster config always get reloaded
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);

    accessor.clearReadCounters();
    // refresh again should read nothing as ideal state is same
    cache.notifyDataChange(HelixConstants.ChangeType.IDEAL_STATE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);

    accessor.clearReadCounters();
    cache.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);
  }

  @Test(dependsOnMethods = {"testUpdateOnNotification"})
  public void testSelectiveUpdates() throws Exception {
    MockZkHelixDataAccessor accessor =
        new MockZkHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    ClusterDataCache cache =
        new ClusterDataCache("CLUSTER_" + TestHelper.getTestClassName());
    cache.refresh(accessor);

    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 1);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), NODE_NR + 1);

    accessor.clearReadCounters();

    // refresh again should read nothing
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);

    // add a new resource
    _setupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_1", _PARTITIONS, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_1", _replica);

    Thread.sleep(100);
    HelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verify());

    accessor.clearReadCounters();

    // refresh again should read only new current states and new idealstate
    cache.notifyDataChange(HelixConstants.ChangeType.IDEAL_STATE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 1);

    // Add more resources
    accessor.clearReadCounters();

    _setupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_2", _PARTITIONS, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_2", _replica);
    _setupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_3", _PARTITIONS, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_3", _replica);

    // Totally four resources. Two of them are newly added.
    cache.notifyDataChange(HelixConstants.ChangeType.IDEAL_STATE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 2);
  }
}
