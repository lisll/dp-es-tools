package com.datapipeline.model;


public class DataTaskDelayMonitorGraph extends DataTaskState {
  private static final long serialVersionUID = 531244780756052742L;

  public static final String DP_TASK_DELAY_GRAPH_INDEX = "dp-historical-task-delay-graph";

  public static final String DP_TASK_DELAY_GRAPH_MAPPING =
      "{ "
          + " \"properties\": {"
          + "   \"taskId\": { "
          + "     \"type\": \"integer\""
          + "   }, "
          + "   \"mappingId\": { "
          + "    \"type\": \"integer\" "
          + "   }, "
          + "   \"batchDelayTime\": { "
          + "    \"type\": \"long\" "
          + "   }, "
          + "   \"createdAt\": { "
          + "    \"type\": \"long\" "
          + "   } "
          + "   }"
          + "  }";

  private Integer taskId;
  private Integer mappingId;
  private Long batchDelayTime;
  private Long createdAt;


  public Integer getTaskId() {
    return taskId;
  }

  public void setTaskId(Integer taskId) {
    this.taskId = taskId;
  }

  public Integer getMappingId() {
    return mappingId;
  }

  public void setMappingId(Integer mappingId) {
    this.mappingId = mappingId;
  }

  public Long getBatchDelayTime() {
    return batchDelayTime;
  }

  public void setBatchDelayTime(Long batchDelayTime) {
    this.batchDelayTime = batchDelayTime;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Long createdAt) {
    this.createdAt = createdAt;
  }
}
