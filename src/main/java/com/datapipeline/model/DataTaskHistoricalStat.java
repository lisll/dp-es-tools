package com.datapipeline.model;

import java.io.Serializable;
import java.util.Objects;

// 民生生产环境mapping结构 一共10个字段
public class DataTaskHistoricalStat implements Serializable {

  private Integer taskId;

  private Integer nodeId;
  /** if isSrc is true , mapping is entityId , else is mappingId */
  private Integer mapping;

  private Integer partition;

  private Boolean src;
  private Double bytesRate;
  private Double countRate;
  private Long bytesSum;
  private Long countSum;

  private Long createdAt;

  public Integer getTaskId() {
    return taskId;
  }

  public void setTaskId(Integer taskId) {
    this.taskId = taskId;
  }

  public Integer getNodeId() {
    return nodeId;
  }

  public void setNodeId(Integer nodeId) {
    this.nodeId = nodeId;
  }

  public Integer getMapping() {
    return mapping;
  }

  public void setMapping(Integer mapping) {
    this.mapping = mapping;
  }

  public Integer getPartition() {
    return partition;
  }

  public void setPartition(Integer partition) {
    this.partition = partition;
  }

  public Boolean getSrc() {
    return src;
  }

  public void setSrc(Boolean src) {
    this.src = src;
  }

  public Double getBytesRate() {
    return bytesRate;
  }

  public void setBytesRate(Double bytesRate) {
    this.bytesRate = bytesRate;
  }

  public Double getCountRate() {
    return countRate;
  }

  public void setCountRate(Double countRate) {
    this.countRate = countRate;
  }

  public Long getBytesSum() {
    return bytesSum;
  }

  public void setBytesSum(Long bytesSum) {
    this.bytesSum = bytesSum;
  }

  public Long getCountSum() {
    return countSum;
  }

  public void setCountSum(Long countSum) {
    this.countSum = countSum;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Long createdAt) {
    this.createdAt = createdAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataTaskHistoricalStat)) {
      return false;
    }
    DataTaskHistoricalStat that = (DataTaskHistoricalStat) o;
    return getTaskId().equals(that.getTaskId())
        && getNodeId().equals(that.getNodeId())
        && getMapping().equals(that.getMapping())
        && getPartition().equals(that.getPartition())
        && getSrc() == that.getSrc()
        && Double.compare(that.getBytesRate(), getBytesRate()) == 0
        && Double.compare(that.getCountRate(), getCountRate()) == 0
        && getBytesSum().equals(that.getBytesSum())
        && getCountSum().equals(that.getCountSum())
        && getCreatedAt().equals(that.getCreatedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getTaskId(),
        getNodeId(),
        getMapping(),
        getPartition(),
        getSrc(),
        getBytesRate(),
        getCountRate(),
        getBytesSum(),
        getCountSum(),
        getCreatedAt());
  }

  public static final String DP_TASK_HISTORICAL_MAPPING =
      "{"
          + "\"properties\": {"
          + "   \"createdAt\": {"
          + "     \"type\": \"long\""
          + "   },"
          + "   \"mapping\": {"
          + "     \"type\": \"integer\""
          + "   },"
          + "   \"bytesRate\": {"
          + "     \"type\": \"double\""
          + "   },"
          + "   \"partition\": {"
          + "     \"type\": \"integer\""
          + "   },"
          + "   \"src\": {"
          + "     \"type\": \"boolean\""
          + "   },"
          + "   \"_class\": {"
          + "     \"index\": false,"
          + "     \"type\": \"keyword\","
          + "     \"doc_values\": false"
          + "   },"
          + "   \"bytesSum\": {"
          + "     \"type\": \"long\""
          + "   },"
          + "   \"nodeId\": {"
          + "     \"type\": \"integer\""
          + "   },"
          + "   \"taskId\": {"
          + "     \"type\": \"integer\""
          + "   },"
          + "   \"countRate\": {"
          + "     \"type\": \"double\""
          + "   },"
          + "   \"countSum\": {"
          + "     \"type\": \"long\""
          + "   }"
          + " }"
          + "}";
}
