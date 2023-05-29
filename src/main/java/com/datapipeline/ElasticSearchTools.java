package com.datapipeline;

import static com.datapipeline.mapping.DataTaskDelayMonitorGraph.DP_TASK_DELAY_GRAPH_INDEX;
import static com.datapipeline.mapping.DataTaskDelayMonitorGraph.DP_TASK_DELAY_GRAPH_MAPPING;
import static com.datapipeline.mapping.DataTaskHistoricalStat.DP_TASK_HISTORICAL_MAPPING;
import static com.datapipeline.mapping.DataTaskHistoricalStat.DP_TASK_PROCESS_INDEX;
import static com.datapipeline.mapping.DataTaskStatisticsInfo.DP_HISTORICAL_MONITOR_STAT_MAPPING;
import static com.datapipeline.mapping.DataTaskStatisticsInfo.DP_STATISTICS_INDEX;
import static com.datapipeline.mapping.ErrorQueueRecord.DP_ERROR_QUEUE_RECORD_INDEX;
import static com.datapipeline.mapping.ErrorQueueRecord.ERROR_QUEUE_RECORD_INDEX;

import com.datapipeline.core.ElasticSearchClient;
import com.datapipeline.core.ElasticSearchConnect;
import com.datapipeline.core.query.ElasticSearchQuery;
import com.datapipeline.mapping.DataTaskDelayMonitorGraph;
import com.datapipeline.mapping.DataTaskHistoricalStat;
import com.datapipeline.mapping.DataTaskState;
import com.datapipeline.mapping.DataTaskStatisticsInfo;
import com.datapipeline.mapping.ErrorQueueRecord;
import com.datapipeline.mapping.ErrorQueueRecordState;
import com.datapipeline.mapping.ErrorQueueType;
import com.datapipeline.utils.DpUtils;
import com.datapipeline.utils.ObjectConvert;
import com.datapipeline.utils.ParameterTool;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

public class ElasticSearchTools {
  public static String indexName = "dp-task-historical-stat";

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("请在命令行中指定配置文件");
      return;
    }
    ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

    Properties propertiesFromLocal = DpUtils.getPropertiesFromLocal(args[0]);
    if (propertiesFromLocal == null) {
      System.out.println(args[0] + ":路径不存在，请确认配置文件是否存在");
      return;
    }

    String specifyIndexName = parameterTool.get("indexName");
    if (StringUtils.isNotBlank(specifyIndexName)) {
      ElasticSearchTools.indexName = specifyIndexName;
    }

    String executeMode = parameterTool.get("executeMode");

    ElasticSearchConnect connect = ElasticSearchClient.getInstance(parameterTool);
    if (Objects.equals(executeMode, "insert")) {
      if (!connect.indexExists(ElasticSearchTools.indexName)) {
        int number_of_shards = Integer.parseInt(parameterTool.get("number_of_shards"));
        int number_of_replicas = Integer.parseInt(parameterTool.get("number_of_replicas"));
        // indexName.contains(DP_TASK_DELAY_GRAPH_INDEX
        if (indexName.contains(DP_TASK_PROCESS_INDEX)) {
          connect.createIndex(
              ElasticSearchTools.indexName,
              DP_TASK_HISTORICAL_MAPPING,
              number_of_shards,
              number_of_replicas);
        } else if (indexName.contains(DP_TASK_DELAY_GRAPH_INDEX)) {
          connect.createIndex(
              ElasticSearchTools.indexName,
              DP_TASK_DELAY_GRAPH_MAPPING,
              number_of_shards,
              number_of_replicas);
        } else if (indexName.contains(DP_STATISTICS_INDEX)) {
          connect.createIndex(
              ElasticSearchTools.indexName,
              DP_HISTORICAL_MONITOR_STAT_MAPPING,
              number_of_shards,
              number_of_replicas);
        } else if (indexName.contains(ERROR_QUEUE_RECORD_INDEX)) {
          connect.createIndex(
              ElasticSearchTools.indexName,
              DP_ERROR_QUEUE_RECORD_INDEX,
              number_of_shards,
              number_of_replicas);
        }
      }
      multi(connect, parameterTool);
    } else if (Objects.equals(executeMode, "delete")) {
      deleteByQuery(connect, parameterTool);
    } else {
//      System.out.println("当前模式不支持:" + executeMode + " ,请重新配置模式");
//      RestHighLevelClient restHighLevelClient = connect.getRestHighLevelClient();
//      ElasticSearchQuery query = new ElasticSearchQuery(restHighLevelClient);
//      query.getTaskDelayMonitorGraph(indexName);
      HashMap<String, String> map = new HashMap<>();
      map.put("state","iiiii");

      connect.update(indexName,"1",map);
    }
  }

  private static void multi(ElasticSearchConnect connect, ParameterTool parameterTool)
      throws Exception {
    int parallelNum = Integer.parseInt(parameterTool.get("parallelNum"));
    int mockBase = Integer.parseInt(parameterTool.get("mockBase"));
    int batchSize = Integer.parseInt(parameterTool.get("batchSize"));
    int recentlyMonth = Integer.parseInt(parameterTool.get("recentlyMonth"));
    int rangeIds = Integer.parseInt(parameterTool.get("rangeIds"));
    List<Thread> list = new ArrayList<>();
    for (int i = 0; i < parallelNum; i++) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  batchBulkSaveDoc(connect, mockBase, batchSize, recentlyMonth, rangeIds);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              },
              "thread-" + i);
      list.add(thread);
      thread.start();
    }
    for (Thread t : list) {
      t.join();
    }
    connect.closeResource();
  }

  private static void batchBulkSaveDoc(
      ElasticSearchConnect connect, int mockBase, int batchSize, int recentlyMonth, int rangeIds)
      throws Exception {
    //    DataTaskStatisticsInfo dataTaskStatisticsInfo = generateTaskStatisticsInfo(recentlyMonth,
    //        rangeIds);
    //
    // connect.upsertDocByPk(indexName,DP_HISTORICAL_MONITOR_STAT_MAPPING,"2",ObjectConvert.getJsonString(dataTaskStatisticsInfo));
    // 单位(万条)
    int mockSize = mockBase * 100;
    int actionCount = mockSize / batchSize;
    int i = 1;
    System.out.printf("总数据量 = %s , 批数据量 = %s， 执行次数 = %s%n", mockSize, batchSize, actionCount);
    System.out.println("插入前，当前线程为:" + Thread.currentThread().getName() + ",当前系统时间: " + new Date());
    while (i < actionCount + 1) {
      System.out.printf("准备第 %s 次执行，%n", i);
      List<Map<String, Object>> sources = new ArrayList<>(batchSize + 10);
      long start = System.currentTimeMillis();
      int j = 0;
      while (j < batchSize) {
        DataTaskState dataTaskHistoricalStat = generateData(recentlyMonth, rangeIds);
        Map<String, Object> source = ObjectConvert.beanToMap(dataTaskHistoricalStat);
        sources.add(source);
        j++;
      }
      long end = System.currentTimeMillis();
      System.out.println("数据准备花费时间(ms) =  " + (end - start));
      System.out.println("准备插入 " + sources.size() + "条数据");
      BulkResponse bulkItemResponses = connect.bulkSaveDoc(indexName, sources);
      System.out.println("插入花费时间 = " + bulkItemResponses.getTook().toString());
      System.out.println("错误日志 = " + bulkItemResponses.buildFailureMessage());
      i++;
      System.out.printf("第 %s 次执行完成，%n", i);
      System.out.print("-------------------- \n");
    }
    //    connect.closeResource();
    System.out.println(
        "当前批次插入完成,任务即将停止,当前线程为:" + Thread.currentThread().getName() + ",当前系统时间: " + new Date());
  }

  private static void batchUpsertDocByPk(
      ElasticSearchConnect connect, int mockBase, int batchSize, int recentlyMonth, int rangeIds)
      throws Exception {
    DataTaskState dataTaskState = generateData(recentlyMonth, rangeIds);
    String param = ObjectConvert.getJsonString(dataTaskState);
    connect.upsertDocByPk(indexName, DP_HISTORICAL_MONITOR_STAT_MAPPING, "3", param);
  }

  private static DataTaskState generateData(int recentlyMonth, int rangeIds) {
    DataTaskState taskState = null;
    if (indexName.contains(DP_TASK_DELAY_GRAPH_INDEX)) {
      taskState = generateTaskDelayMonitorGraph(recentlyMonth, rangeIds);
    } else if (indexName.contains(DP_TASK_PROCESS_INDEX)) {
      taskState = generateTaskHistoricalStat(recentlyMonth, rangeIds);
    } else if (indexName.contains(DP_STATISTICS_INDEX)) {
      taskState = generateTaskStatisticsInfo(recentlyMonth, rangeIds);
    } else if (indexName.contains(ERROR_QUEUE_RECORD_INDEX)) {
      taskState = generateErrorQueueRecord(recentlyMonth, rangeIds);
    }
    if (taskState == null) {
      throw new RuntimeException("current indexName is error please check it ");
    }
    return taskState;
  }

  private static DataTaskHistoricalStat generateTaskHistoricalStat(
      int recentlyMonth, int rangeIds) {
    DataTaskHistoricalStat dataTaskHistoricalStat = new DataTaskHistoricalStat();
    Random rd = new Random();
    dataTaskHistoricalStat.setTaskId(rd.nextInt(rangeIds) + 1);
    dataTaskHistoricalStat.setMapping(rd.nextInt(2000) + 1);
    dataTaskHistoricalStat.setNodeId(rd.nextInt(500) + 1);
    dataTaskHistoricalStat.setPartition(0);
    dataTaskHistoricalStat.setSrc(rd.nextBoolean());
    dataTaskHistoricalStat.setBytesRate(0.0);
    dataTaskHistoricalStat.setCountRate(0.0);
    dataTaskHistoricalStat.setBytesSum(rd.nextInt(200000) + 1L);
    dataTaskHistoricalStat.setCountSum(rd.nextInt(20000) + 1L);
    long priorTime = DpUtils.getPriorTime(recentlyMonth);
    long randomTime = ThreadLocalRandom.current().nextLong(priorTime, new Date().getTime());
    dataTaskHistoricalStat.setCreatedAt(randomTime);
    return dataTaskHistoricalStat;
  }

  private static DataTaskDelayMonitorGraph generateTaskDelayMonitorGraph(
      int recentlyMonth, int rangeIds) {
    DataTaskDelayMonitorGraph dataTaskDelayMonitorGraph = new DataTaskDelayMonitorGraph();
    Random rd = new Random();
    dataTaskDelayMonitorGraph.setTaskId(rd.nextInt(rangeIds) + 1);
    dataTaskDelayMonitorGraph.setMappingId(rd.nextInt(5) + 1);
    dataTaskDelayMonitorGraph.setBatchDelayTime(Long.parseLong(rd.nextInt(1000000) + ""));
    long priorTime = DpUtils.getPriorTime(recentlyMonth);
    long randomTime = ThreadLocalRandom.current().nextLong(priorTime, new Date().getTime());
    dataTaskDelayMonitorGraph.setCreatedAt(randomTime);
    return dataTaskDelayMonitorGraph;
  }

  private static DataTaskStatisticsInfo generateTaskStatisticsInfo(
      int recentlyMonth, int rangeIds) {
    Random rd = new Random();
    DataTaskStatisticsInfo dataTaskStatisticsInfo = new DataTaskStatisticsInfo();
    dataTaskStatisticsInfo.setTaskId(rd.nextInt(rangeIds) + 1);
    dataTaskStatisticsInfo.setMappingId(rd.nextInt(2000) + 1);
    dataTaskStatisticsInfo.setProcessedRecords(888);
    dataTaskStatisticsInfo.setTotalProcessedRecords(999);
    dataTaskStatisticsInfo.setTotalRecords(20);
    dataTaskStatisticsInfo.setRemainingTime(100);
    dataTaskStatisticsInfo.setFullDone(false);
    //    dataTaskStatisticsInfo.setLastDayMaxDelayTime("2023-04-10_-1");
    //    dataTaskStatisticsInfo.setTodayMaxDelayTime("2023-04-10_0");
    dataTaskStatisticsInfo.setUpdateAt("2023-04-10");
    long priorTime = DpUtils.getPriorTime(recentlyMonth);
    long randomTime = ThreadLocalRandom.current().nextLong(priorTime, new Date().getTime());
    dataTaskStatisticsInfo.setCreatedAt(randomTime);
    return dataTaskStatisticsInfo;
  }

  private static ErrorQueueRecord generateErrorQueueRecord(int recentlyMonth, int rangeIds) {
    Random rd = new Random();
    ErrorQueueRecord errorQueueRecord = new ErrorQueueRecord();
//    errorQueueRecord.setTaskId(rd.nextInt(rangeIds) + 1);
    errorQueueRecord.setTaskId(15);
    errorQueueRecord.setErrorRecordJson("RecordJson");
    errorQueueRecord.setErrorFieldName("ErrorFieldName");
    errorQueueRecord.setOccurTime(System.currentTimeMillis());
//    errorQueueRecord.setActionHandleUuid("ActionHandleUuid");
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 227191; i++) {
      stringBuilder.append("hello" + i);
    }
    errorQueueRecord.setErrorRecordJson(stringBuilder.toString());
    errorQueueRecord.setSrcEntityId(30109);
    errorQueueRecord.setSinkEntityId(32596);
    int temp = rd.nextInt(10);
    if (temp == 0 || temp == 1) {
      errorQueueRecord.setErrorType(ErrorQueueType.NON_NULL_VIOLATION);
    } else if (temp == 2 || temp == 3) {
      errorQueueRecord.setErrorType(ErrorQueueType.DATA_OVERFLOW);
    } else {
      errorQueueRecord.setErrorType(ErrorQueueType.UNKNOWN);
    }
    errorQueueRecord.setState(ErrorQueueRecordState.UNRESOLVED);
    errorQueueRecord.setValue(
        "{\n"
            + "  \"schema\": {\n"
            + "    \"type\": \"struct\",\n"
            + "    \"fields\": [\n"
            + "      {\n"
            + "        \"type\": \"struct\",\n"
            + "        \"fields\": [\n"
            + "          {\n"
            + "            \"type\": \"int32\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"id\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"first_name\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"last_name\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"email\"\n"
            + "          }\n"
            + "        ],\n"
            + "        \"optional\": true,\n"
            + "        \"name\": \"PostgreSQL_server.inventory.customers.Value\",\n"
            + "        \"field\": \"before\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"struct\",\n"
            + "        \"fields\": [\n"
            + "          {\n"
            + "            \"type\": \"int32\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"id\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"first_name\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"last_name\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"email\"\n"
            + "          }\n"
            + "        ],\n"
            + "        \"optional\": true,\n"
            + "        \"name\": \"PostgreSQL_server.inventory.customers.Value\",\n"
            + "        \"field\": \"after\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"struct\",\n"
            + "        \"fields\": [\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"version\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"connector\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"name\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"int64\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"ts_ms\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"boolean\",\n"
            + "            \"optional\": true,\n"
            + "            \"default\": false,\n"
            + "            \"field\": \"snapshot\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"db\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"schema\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"string\",\n"
            + "            \"optional\": false,\n"
            + "            \"field\": \"table\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"int64\",\n"
            + "            \"optional\": true,\n"
            + "            \"field\": \"txId\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"int64\",\n"
            + "            \"optional\": true,\n"
            + "            \"field\": \"lsn\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"boolean\",\n"
            + "            \"optional\": true,\n"
            + "            \"field\": \"eqr\"\n"
            + "          },\n"
            + "          {\n"
            + "            \"type\": \"int64\",\n"
            + "            \"optional\": true,\n"
            + "            \"field\": \"xmin\"\n"
            + "          }\n"
            + "        ],\n"
            + "        \"optional\": false,\n"
            + "        \"name\": \"io.debezium.connector.postgresql.Source\",\n"
            + "        \"field\": \"source\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"string\",\n"
            + "        \"optional\": false,\n"
            + "        \"field\": \"op\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"type\": \"int64\",\n"
            + "        \"optional\": true,\n"
            + "        \"field\": \"ts_ms\"\n"
            + "      }\n"
            + "    ],\n"
            + "    \"optional\": false,\n"
            + "    \"name\": \"PostgreSQL_server.inventory.customers.Envelope\"\n"
            + "  },\n"
            + "  \"payload\": {\n"
            + "    \"before\": null,\n"
            + "    \"after\": {\n"
            + "      \"id\": 1,\n"
            + "      \"first_name\": \"Anne\",\n"
            + "      \"last_name\": \"Kretchmar\",\n"
            + "      \"email\": \"annek@noanswer.org\"\n"
            + "    },\n"
            + "    \"source\": {\n"
            + "      \"version\": \"1.2.5.Final\",\n"
            + "      \"connector\": \"postgresql\",\n"
            + "      \"name\": \"PostgreSQL_server\",\n"
            + "      \"ts_ms\": 1559033904863,\n"
            + "      \"snapshot\": true,\n"
            + "      \"db\": \"postgres\",\n"
            + "      \"schema\": \"public\",\n"
            + "      \"table\": \"customers\",\n"
            + "      \"txId\": 555,\n"
            + "      \"lsn\": 24023128,\n"
            + "      \"xmin\": null\n"
            + "    },\n"
            + "    \"op\": \"c\",\n"
            + "    \"ts_ms\": 1559033904863\n"
            + "  }\n"
            + "}");
    errorQueueRecord.setTopicName("dblab04");
    long priorTime = DpUtils.getPriorTime(recentlyMonth);
    long randomTime = ThreadLocalRandom.current().nextLong(priorTime, new Date().getTime());
    errorQueueRecord.setCreatedAt(randomTime);
    return errorQueueRecord;
  }

  private static void deleteByQuery(ElasticSearchConnect connect, ParameterTool parameterTool)
      throws Exception {
    long start = System.currentTimeMillis();
    System.out.println("准备删除索引: " + indexName + " 下的部分数据, " + new Date());
    boolean isDeleteAll = Boolean.parseBoolean(parameterTool.get("isDeleteAll"));
    String deleteField = parameterTool.get("deleteField");
    String deleteValue = parameterTool.get(deleteField);
    ArrayList<String> taskIds = new ArrayList<>();
    if (deleteValue.startsWith("[") && deleteValue.endsWith("]")) {
      String temp = deleteValue.substring(deleteValue.indexOf("[") + 1, deleteValue.indexOf("]"));
      if (deleteValue.contains(",")) {
        String[] split = temp.split(",");
        for (String s : split) {
          taskIds.add(s);
        }
      } else if (deleteValue.contains("-")) {
        String[] split = temp.split("-");
        int startPoint = Integer.parseInt(split[0]);
        int endPoint = Integer.parseInt(split[1]);
        for (int i = startPoint; i <= endPoint; i++) {
          taskIds.add(i + "");
        }
      } else {
        taskIds.add(temp);
      }
    }
    for (String taskId : taskIds) {
      String startTime = parameterTool.get("startTime");
      String endTime = parameterTool.get("endTime");
      int scrollSize = Integer.parseInt(parameterTool.get("scroll_size"));
      int slicesSize = Integer.parseInt(parameterTool.get("slices_size"));
      int scrollKeepAlive = Integer.parseInt(parameterTool.get("scroll_keep_alive"));
      QueryBuilder queryBuilder =
          builderQuery(isDeleteAll, deleteField, taskId, startTime, endTime);
      BulkByScrollResponse bulkByScrollResponse =
          connect.deleteDocByQuery(
              indexName, queryBuilder, scrollSize, slicesSize, scrollKeepAlive);
      long deleted = bulkByScrollResponse.getDeleted();
      long total = bulkByScrollResponse.getTotal();
      TimeValue took = bulkByScrollResponse.getTook();
      //      connect.closeResource();
      long end = System.currentTimeMillis();
      System.out.println("删除完成...." + new Date());
      System.out.println("此次请求处理的文档总数为: " + total);
      System.out.println("TimeValue: " + took.getSeconds());
      System.out.println("删除 " + deleted + " 条数据，所用时间为:" + (end - start) / 1000 + " s");
    }
    connect.closeResource();
  }

  // 目前只构建针对 CreatedAt 的区间查询
  private static QueryBuilder builderQuery(
      boolean deleteAll, String deleteField, String taskId, String startTime, String endTime) {
    BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
    QueryBuilder prefixBuilder = null;
    if (StringUtils.isNotBlank(taskId)) {
      prefixBuilder = QueryBuilders.termQuery(deleteField, Integer.parseInt(taskId));
      prefixBuilder.queryName();
    }
    if (deleteAll) {
      return QueryBuilders.matchAllQuery();
    }
    // 闭区间查询 从开始时间-结束时间
    if (StringUtils.isNotBlank(startTime) && StringUtils.isNotBlank(endTime)) {
      boolQueryBuilder.must(
          QueryBuilders.rangeQuery("createdAt")
              .from(DpUtils.parseTime(startTime))
              .to(DpUtils.parseTime(endTime)));
      if (prefixBuilder != null) {
        boolQueryBuilder.must(prefixBuilder);
      }
    }
    // 大于等于某个时间点的数据
    if (StringUtils.isNotBlank(startTime) && !StringUtils.isNotBlank(endTime)) {
      boolQueryBuilder.must(
          QueryBuilders.rangeQuery("createdAt").gte(DpUtils.parseTime(startTime)));
      if (prefixBuilder != null) {
        boolQueryBuilder.must(prefixBuilder);
      }
    }
    // 小于等于某个时间点的数据
    if (!StringUtils.isNotBlank(startTime) && StringUtils.isNotBlank(endTime)) {
      boolQueryBuilder.must(QueryBuilders.rangeQuery("createdAt").lte(DpUtils.parseTime(endTime)));
      boolQueryBuilder.must(prefixBuilder);
    }
    System.out.println("------------ 构建的queryBuilder如下 -----------");
    if (boolQueryBuilder instanceof BoolQueryBuilder) {
      List<QueryBuilder> must = ((BoolQueryBuilder) boolQueryBuilder).must();
      for (QueryBuilder builder : must) {
        if (builder instanceof TermQueryBuilder) {
          TermQueryBuilder term = (TermQueryBuilder) builder;
          System.out.println("term: " + term.fieldName() + ":" + term.value());
        }
      }
    }
    System.out.println(boolQueryBuilder);
    return boolQueryBuilder;
  }
}
