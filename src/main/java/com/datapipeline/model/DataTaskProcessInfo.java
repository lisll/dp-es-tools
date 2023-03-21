package com.datapipeline.model;

import java.io.Serializable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DataTaskProcessInfo implements Serializable {

  private int taskId;    //

  private int nodeId;  //
  /** if isSrc is true , mapping is entityId , else is mappingId */
  private int mapping;  //

  private int partition;  //

  private long srcEntityId;
  private boolean src;   //
  private double bytesRate;   //
  private double countRate;   //
  private long bytesSum;     //
  private long countSum;    //
  private long insertBytesSum;
  private long insertCountSum;
  private long updateBytesSum;
  private long updateCountSum;
  private long deleteBytesSum;
  private long deleteCountSum;
  private long mergeBytesSum;
  private long mergeCountSum;
  private long ignoreBytesSum;
  private long ignoreCountSum;
  private long syncedInsertBytesSum;
  private long syncedInsertCountSum;
  private long syncedUpdateBytesSum;
  private long syncedUpdateCountSum;
  private long syncedDeleteBytesSum;
  private long syncedDeleteCountSum;
  private long transCountSum;
  private long lastReadTs;   //
  private long createdAt;  //

  public static final String DP_TASK_PROCESS_MAPPING =
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
          + "   },"
          + "   \"lastReadTs\": {"
          + "     \"type\": \"long\""
          + "   }"
          + " }"
          + "}";

  public DataTaskProcessInfo(
      int taskId,
      int nodeId,
      int mapping,
      int partition,
      long srcEntityId,
      boolean src,
      double bytesRate,
      double countRate,
      long bytesSum,
      long countSum,
      long insertBytesSum,
      long insertCountSum,
      long updateBytesSum,
      long updateCountSum,
      long deleteBytesSum,
      long deleteCountSum,
      long mergeBytesSum,
      long mergeCountSum,
      long ignoreBytesSum,
      long ignoreCountSum,
      long syncedInsertBytesSum,
      long syncedInsertCountSum,
      long syncedUpdateBytesSum,
      long syncedUpdateCountSum,
      long syncedDeleteBytesSum,
      long syncedDeleteCountSum,
      long transCountSum,
      long lastReadTs,
      long createdAt) {
    this.taskId = taskId;
    this.nodeId = nodeId;
    this.mapping = mapping;
    this.partition = partition;
    this.srcEntityId = srcEntityId;
    this.src = src;
    this.bytesRate = bytesRate;
    this.countRate = countRate;
    this.bytesSum = bytesSum;
    this.countSum = countSum;
    this.insertBytesSum = insertBytesSum;
    this.insertCountSum = insertCountSum;
    this.updateBytesSum = updateBytesSum;
    this.updateCountSum = updateCountSum;
    this.deleteBytesSum = deleteBytesSum;
    this.deleteCountSum = deleteCountSum;
    this.mergeBytesSum = mergeBytesSum;
    this.mergeCountSum = mergeCountSum;
    this.ignoreBytesSum = ignoreBytesSum;
    this.ignoreCountSum = ignoreCountSum;
    this.syncedInsertBytesSum = syncedInsertBytesSum;
    this.syncedInsertCountSum = syncedInsertCountSum;
    this.syncedUpdateBytesSum = syncedUpdateBytesSum;
    this.syncedUpdateCountSum = syncedUpdateCountSum;
    this.syncedDeleteBytesSum = syncedDeleteBytesSum;
    this.syncedDeleteCountSum = syncedDeleteCountSum;
    this.transCountSum = transCountSum;
    this.lastReadTs = lastReadTs;
    this.createdAt = createdAt;
  }

  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public int getMapping() {
    return mapping;
  }

  public void setMapping(int mapping) {
    this.mapping = mapping;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getSrcEntityId() {
    return srcEntityId;
  }

  public void setSrcEntityId(long srcEntityId) {
    this.srcEntityId = srcEntityId;
  }

  public boolean isSrc() {
    return src;
  }

  public void setSrc(boolean src) {
    this.src = src;
  }

  public double getBytesRate() {
    return bytesRate;
  }

  public void setBytesRate(double bytesRate) {
    this.bytesRate = bytesRate;
  }

  public double getCountRate() {
    return countRate;
  }

  public void setCountRate(double countRate) {
    this.countRate = countRate;
  }

  public long getBytesSum() {
    return bytesSum;
  }

  public void setBytesSum(long bytesSum) {
    this.bytesSum = bytesSum;
  }

  public long getCountSum() {
    return countSum;
  }

  public void setCountSum(long countSum) {
    this.countSum = countSum;
  }

  public long getInsertBytesSum() {
    return insertBytesSum;
  }

  public void setInsertBytesSum(long insertBytesSum) {
    this.insertBytesSum = insertBytesSum;
  }

  public long getInsertCountSum() {
    return insertCountSum;
  }

  public void setInsertCountSum(long insertCountSum) {
    this.insertCountSum = insertCountSum;
  }

  public long getUpdateBytesSum() {
    return updateBytesSum;
  }

  public void setUpdateBytesSum(long updateBytesSum) {
    this.updateBytesSum = updateBytesSum;
  }

  public long getUpdateCountSum() {
    return updateCountSum;
  }

  public void setUpdateCountSum(long updateCountSum) {
    this.updateCountSum = updateCountSum;
  }

  public long getDeleteBytesSum() {
    return deleteBytesSum;
  }

  public void setDeleteBytesSum(long deleteBytesSum) {
    this.deleteBytesSum = deleteBytesSum;
  }

  public long getDeleteCountSum() {
    return deleteCountSum;
  }

  public void setDeleteCountSum(long deleteCountSum) {
    this.deleteCountSum = deleteCountSum;
  }

  public long getMergeBytesSum() {
    return mergeBytesSum;
  }

  public void setMergeBytesSum(long mergeBytesSum) {
    this.mergeBytesSum = mergeBytesSum;
  }

  public long getMergeCountSum() {
    return mergeCountSum;
  }

  public void setMergeCountSum(long mergeCountSum) {
    this.mergeCountSum = mergeCountSum;
  }

  public long getIgnoreBytesSum() {
    return ignoreBytesSum;
  }

  public void setIgnoreBytesSum(long ignoreBytesSum) {
    this.ignoreBytesSum = ignoreBytesSum;
  }

  public long getIgnoreCountSum() {
    return ignoreCountSum;
  }

  public void setIgnoreCountSum(long ignoreCountSum) {
    this.ignoreCountSum = ignoreCountSum;
  }

  public long getSyncedInsertBytesSum() {
    return syncedInsertBytesSum;
  }

  public void setSyncedInsertBytesSum(long syncedInsertBytesSum) {
    this.syncedInsertBytesSum = syncedInsertBytesSum;
  }

  public long getSyncedInsertCountSum() {
    return syncedInsertCountSum;
  }

  public void setSyncedInsertCountSum(long syncedInsertCountSum) {
    this.syncedInsertCountSum = syncedInsertCountSum;
  }

  public long getSyncedUpdateBytesSum() {
    return syncedUpdateBytesSum;
  }

  public void setSyncedUpdateBytesSum(long syncedUpdateBytesSum) {
    this.syncedUpdateBytesSum = syncedUpdateBytesSum;
  }

  public long getSyncedUpdateCountSum() {
    return syncedUpdateCountSum;
  }

  public void setSyncedUpdateCountSum(long syncedUpdateCountSum) {
    this.syncedUpdateCountSum = syncedUpdateCountSum;
  }

  public long getSyncedDeleteBytesSum() {
    return syncedDeleteBytesSum;
  }

  public void setSyncedDeleteBytesSum(long syncedDeleteBytesSum) {
    this.syncedDeleteBytesSum = syncedDeleteBytesSum;
  }

  public long getSyncedDeleteCountSum() {
    return syncedDeleteCountSum;
  }

  public void setSyncedDeleteCountSum(long syncedDeleteCountSum) {
    this.syncedDeleteCountSum = syncedDeleteCountSum;
  }

  public long getTransCountSum() {
    return transCountSum;
  }

  public void setTransCountSum(long transCountSum) {
    this.transCountSum = transCountSum;
  }

  public long getLastReadTs() {
    return lastReadTs;
  }

  public void setLastReadTs(long lastReadTs) {
    this.lastReadTs = lastReadTs;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof DataTaskProcessInfo)) {
      return false;
    }

    DataTaskProcessInfo that = (DataTaskProcessInfo) o;

    return new EqualsBuilder()
        .append(taskId, that.taskId)
        .append(nodeId, that.nodeId)
        .append(mapping, that.mapping)
        .append(partition, that.partition)
        .append(src, that.src)
        .append(bytesRate, that.bytesRate)
        .append(countRate, that.countRate)
        .append(bytesSum, that.bytesSum)
        .append(countSum, that.countSum)
        .append(insertBytesSum, that.insertBytesSum)
        .append(insertCountSum, that.insertCountSum)
        .append(updateBytesSum, that.updateBytesSum)
        .append(updateCountSum, that.updateCountSum)
        .append(deleteBytesSum, that.deleteBytesSum)
        .append(deleteCountSum, that.deleteCountSum)
        .append(mergeBytesSum, that.mergeBytesSum)
        .append(mergeCountSum, that.mergeCountSum)
        .append(ignoreBytesSum, that.ignoreBytesSum)
        .append(ignoreCountSum, that.ignoreCountSum)
        .append(syncedInsertBytesSum, that.syncedInsertBytesSum)
        .append(syncedInsertBytesSum, that.syncedInsertBytesSum)
        .append(syncedUpdateBytesSum, that.syncedUpdateBytesSum)
        .append(syncedUpdateCountSum, that.syncedUpdateCountSum)
        .append(syncedDeleteBytesSum, that.syncedDeleteBytesSum)
        .append(syncedDeleteCountSum, that.syncedDeleteCountSum)
        .append(transCountSum, that.transCountSum)
        .append(lastReadTs, that.lastReadTs)
        .append(createdAt, that.createdAt)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(taskId)
        .append(nodeId)
        .append(mapping)
        .append(partition)
        .append(src)
        .append(bytesRate)
        .append(countRate)
        .append(bytesSum)
        .append(countSum)
        .append(insertBytesSum)
        .append(insertCountSum)
        .append(updateBytesSum)
        .append(updateCountSum)
        .append(deleteBytesSum)
        .append(deleteCountSum)
        .append(mergeBytesSum)
        .append(mergeCountSum)
        .append(ignoreBytesSum)
        .append(ignoreCountSum)
        .append(syncedInsertBytesSum)
        .append(syncedInsertCountSum)
        .append(syncedUpdateBytesSum)
        .append(syncedUpdateCountSum)
        .append(syncedDeleteBytesSum)
        .append(syncedDeleteCountSum)
        .append(transCountSum)
        .append(lastReadTs)
        .append(createdAt)
        .toHashCode();
  }

  public DataTaskProcessInfo() {}

  public String generateMetricKey() {
    return this.taskId + "-" + this.mapping + "-" + this.src + "-" + this.partition;
  }

  public static String generateMetricKey(int taskId, int mappingId, boolean src, int partition) {
    return taskId + "-" + mappingId + "-" + src + "-" + partition;
  }
}
