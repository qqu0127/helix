package org.apache.helix.rest.server.json.cluster;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * POJO class that can be easily convert to JSON object
 * The Cluster Topology represents the hierarchy of the cluster:
 * Cluster
 * - Zone
 * -- Rack(Optional)
 * --- Instance
 * Each layer consists its id and metadata
 */
public class ClusterTopology {
  @JsonProperty("id")
  private final String clusterId;
  @JsonProperty("zones")
  private List<Zone> zones;

  public ClusterTopology(String clusterId, List<Zone> zones) {
    this.clusterId = clusterId;
    this.zones = zones;
  }

  public String getClusterId() {
    return clusterId;
  }

  public List<Zone> getZones() {
    return zones;
  }

  public static final class Zone {
    @JsonProperty("id")
    private final String id;
    @JsonProperty("instances")
    private List<Instance> instances;

    public Zone(String id) {
      this.id = id;
    }

    public Zone(String id, List<Instance> instances) {
      this.id = id;
      this.instances = instances;
    }

    public List<Instance> getInstances() {
      return instances;
    }

    public void setInstances(List<Instance> instances) {
      this.instances = instances;
    }
  }

  public static final class Instance {
    @JsonProperty("id")
    private final String id;

    public Instance(String id) {
      this.id = id;
    }
  }
}
