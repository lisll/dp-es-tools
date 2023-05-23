package com.datapipeline.mapping;

public enum ErrorQueueType {
  DATA_TOO_LONG("数据过长错误"),
  INVALID_FORMAT("非法数据格式错误"),
  NON_NULL_VIOLATION("非空约束错误"),
  NULL_PRIMARY_KEY("主键为空错误"),
  CUSTOMIZED_CODE_ENGINE_ERROR("高级清洗错误"),
  DATA_OVERFLOW("数据越界错误"),
  CONSTRAINT_CONFLICT("主键/唯一索引冲突错误"),
  UNKNOWN("未知错误"),
  INVALID_MSG("非法数据内容错误");

  private String displayMessage;

  ErrorQueueType(String displayMessage) {
    this.displayMessage = displayMessage;
  }

  public String getDisplayMessage() {
    return displayMessage;
  }
}
