package org.apache.helix.rest.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.junit.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestZoneAccessor extends AbstractTestClass  {

  private static final String TEST_CLUSTER = "TestCluster_1";
  @BeforeTest
  public void beforeTest() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(TEST_CLUSTER);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");
    clusterConfig.setTopologyAwareEnabled(true);
    _configAccessor.updateClusterConfig(TEST_CLUSTER, clusterConfig);

    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(TEST_CLUSTER, _baseAccessor);
    Set<String> instances = new HashSet<>();
    // setup up 10 instances across 5 zones
    for (int i = 0; i < 10; i++) {
      String instanceName = TEST_CLUSTER + "_localhost_" + (12918 + i);
      _gSetupTool.addInstanceToCluster(TEST_CLUSTER, instanceName);
      InstanceConfig instanceConfig =
          helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().instanceConfig(instanceName));
      instanceConfig.setDomain("zone=zone_" + i / 2 + ",instance=" + instanceName);
      helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().instanceConfig(instanceName), instanceConfig);
      instances.add(instanceName);
    }
    startInstances(TEST_CLUSTER, instances, 10);
  }

  @Test
  public void testDisabledZones() throws JsonProcessingException {
    String zonesUrl = String.format("clusters/%s/zones", TEST_CLUSTER);
    String disableUrl = String.format("clusters/%s/zones/disabledZones", TEST_CLUSTER);

    Set<String> zones = getSetFromRest(zonesUrl);
    Assert.assertEquals(zones.size(), 5);

    Set<String> disabledZones = getSetFromRest(disableUrl);
    Assert.assertTrue(disabledZones.isEmpty());

    post(zonesUrl + "/zone_2",
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());

    disabledZones = getSetFromRest(disableUrl);
    Assert.assertEquals(disabledZones.size(), 1);
    Assert.assertTrue(disabledZones.contains("zone_2"));

    post(zonesUrl + "/zone_1",
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(zonesUrl + "/zone_2",
        ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(zonesUrl + "/zone_3",
        ImmutableMap.of("command", "disable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    post(zonesUrl + "/zone_4",
        ImmutableMap.of("command", "enable"),
        Entity.entity("", MediaType.APPLICATION_JSON_TYPE),
        Response.Status.OK.getStatusCode());
    disabledZones = getSetFromRest(disableUrl);
    Assert.assertEquals(disabledZones.size(), 2);
    Assert.assertTrue(disabledZones.contains("zone_1"));
    Assert.assertTrue(disabledZones.contains("zone_3"));
  }

  private Set<String> getSetFromRest(String url) throws JsonProcessingException {
    String response = get(url, null, Response.Status.OK.getStatusCode(), true);
    return OBJECT_MAPPER.readValue(response, new TypeReference<HashSet<String>>() { });
  }
}
