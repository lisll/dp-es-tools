package com.datapipeline.mapping;

public class ErrorQueueRecord extends DataTaskState {

  private static final long serialVersionUID = -468038987962336752L;

  public static final String ERROR_QUEUE_RECORD_INDEX = "dp-error-queue-record";

  // 这是民生环境的索引
  public static final String DP_ERROR_QUEUE_RECORD_INDEX =
      "{"
          + "\"properties\": {"
          + "   \"taskId\": {"
          + "     \"type\": \"integer\""
          + "   },"
          + "   \"topicName\": {"
          + "     \"type\": \"text\""
          + "   },"
          + "   \"srcEntityId\": {"
          + "     \"type\": \"integer\""
          + "   },"
          + "   \"sinkEntityId\": {"
          + "     \"type\": \"integer\""
          + "   },"
          + "   \"errorType\": {"
          + "     \"type\": \"keyword\""
          + "   },"
          + "   \"_class\": {"
          + "     \"index\": false,"
          + "     \"type\": \"keyword\","
          + "     \"doc_values\": false"
          + "   },"
          + "   \"stacktrace\": {"
          + "     \"type\": \"text\""
          + "   },"
          + "   \"errorId\": {"
          + "     \"type\": \"keyword\""
          + "   },"
          + "   \"errorFieldName\": {"
          + "     \"type\": \"keyword\""
          + "   },"
          + "   \"errorRecordJson\": {"
          + "     \"type\": \"text\""
          + "   },"
          + "   \"key\": {"
          + "     \"type\": \"text\""
          + "   },"
          + "   \"value\": {"
          + "     \"type\": \"text\""
          + "   },"
          + "   \"occurTime\": {"
          + "     \"type\": \"long\""
          + "   },"
          + "   \"supportActions\": {"
          + "     \"type\": \"text\""
          + "   },"
          + "   \"state\": {"
          + "     \"type\": \"keyword\""
          + "   },"
          + "   \"createdAt\": {"
          + "     \"type\": \"long\""
          + "   },"
          + "   \"actionHandleUuid\": {"
          + "     \"type\": \"keyword\""
          + "   }"
          + " }"
          + "}";

  private int taskId;

  private String topicName;

  private int srcEntityId;

  private int sinkEntityId;

  private ErrorQueueType errorType;

  private String stacktrace;

  private String errorId;

  private String errorFieldName;

  private String errorRecordJson;

  private String key;

  private String value;

  private long occurTime;

  private String supportActions;

  private ErrorQueueRecordState state;

  private String actionHandleUuid;

  private long createdAt;

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public int getSrcEntityId() {
    return srcEntityId;
  }

  public void setSrcEntityId(int srcEntityId) {
    this.srcEntityId = srcEntityId;
  }

  public int getSinkEntityId() {
    return sinkEntityId;
  }

  public void setSinkEntityId(int sinkEntityId) {
    this.sinkEntityId = sinkEntityId;
  }

  public ErrorQueueType getErrorType() {
    return errorType;
  }

  public void setErrorType(ErrorQueueType errorType) {
    this.errorType = errorType;
  }

  public String getStacktrace() {
    return stacktrace;
  }

  public void setStacktrace(String stacktrace) {
    this.stacktrace = stacktrace;
  }

  public String getErrorId() {
    return errorId;
  }

  public void setErrorId(String errorId) {
    this.errorId = errorId;
  }

  public String getErrorFieldName() {
    return errorFieldName;
  }

  public void setErrorFieldName(String errorFieldName) {
    this.errorFieldName = errorFieldName;
  }

  public String getErrorRecordJson() {
    return errorRecordJson;
  }

  public void setErrorRecordJson(String errorRecordJson) {
    this.errorRecordJson = errorRecordJson;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public long getOccurTime() {
    return occurTime;
  }

  public void setOccurTime(long occurTime) {
    this.occurTime = occurTime;
  }

  public String getSupportActions() {
    return supportActions;
  }

  public void setSupportActions(String supportActions) {
    this.supportActions = supportActions;
  }

  public ErrorQueueRecordState getState() {
    return state;
  }

  public void setState(ErrorQueueRecordState state) {
    this.state = state;
  }

  public String getActionHandleUuid() {
    return actionHandleUuid;
  }

  public void setActionHandleUuid(String actionHandleUuid) {
    this.actionHandleUuid = actionHandleUuid;
  }
}
