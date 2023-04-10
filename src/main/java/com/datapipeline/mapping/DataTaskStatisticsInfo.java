package com.datapipeline.mapping;

public class DataTaskStatisticsInfo extends DataTaskState {

  private static final long serialVersionUID = 5687712766707307201L;

  public static final String DP_STATISTICS_INDEX = "dp-historical-monitor-stat";

  public static final String DP_HISTORICAL_MONITOR_STAT_MAPPING =
      "{"
          + "\"properties\": {"
          + "\"mappingId\": {"
          + "   \"type\": \"integer\""
          + "  },"
          + "\"createdAt\": {"
          + "   \"type\": \"long\""
          + "  },"
          + "\"totalRecords\": {"
          + "   \"type\": \"long\""
          + "  },"
          + "\"processedRecords\": {"
          + "   \"type\": \"long\""
          + "  },"
          + "\"todayMaxDelayTime\": {"
          + "   \"type\": \"text\""
          + "  },"
          + "\"updateAt\": {"
          + "   \"type\": \"date\""
          + "  },"
          + "\"totalProcessedRecords\": {"
          + "   \"type\": \"long\""
          + "  },"
          + "\"taskId\": {"
          + "   \"type\": \"integer\""
          + "  },"
          + "\"fullDone\": {"
          + "   \"type\": \"boolean\""
          + "  },"
          + "\"remainingTime\": {"
          + "   \"type\": \"long\""
          + "  }"
          + "}"
          + "}";

  private int taskId;

  private int mappingId;

  private String lastDayMaxDelayTime;

  private String todayMaxDelayTime;

  private int processedRecords;

  private long totalProcessedRecords;

  private long totalRecords;

  // 0:Wait -1:Increment >0:Full
  private long remainingTime;

  private boolean fullDone;

  private String updateAt;

  private long createdAt;

  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public int getMappingId() {
    return mappingId;
  }

  public void setMappingId(int mappingId) {
    this.mappingId = mappingId;
  }

  public String getLastDayMaxDelayTime() {
    return lastDayMaxDelayTime;
  }

  public void setLastDayMaxDelayTime(String lastDayMaxDelayTime) {
    this.lastDayMaxDelayTime = lastDayMaxDelayTime;
  }

  public String getTodayMaxDelayTime() {
    return todayMaxDelayTime;
  }

  public void setTodayMaxDelayTime(String todayMaxDelayTime) {
    this.todayMaxDelayTime = todayMaxDelayTime;
  }

  public int getProcessedRecords() {
    return processedRecords;
  }

  public void setProcessedRecords(int processedRecords) {
    this.processedRecords = processedRecords;
  }

  public long getTotalProcessedRecords() {
    return totalProcessedRecords;
  }

  public void setTotalProcessedRecords(long totalProcessedRecords) {
    this.totalProcessedRecords = totalProcessedRecords;
  }

  public long getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public long getRemainingTime() {
    return remainingTime;
  }

  public void setRemainingTime(long remainingTime) {
    this.remainingTime = remainingTime;
  }

  public boolean isFullDone() {
    return fullDone;
  }

  public void setFullDone(boolean fullDone) {
    this.fullDone = fullDone;
  }

  public String getUpdateAt() {
    return updateAt;
  }

  public void setUpdateAt(String updateAt) {
    this.updateAt = updateAt;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }
}
