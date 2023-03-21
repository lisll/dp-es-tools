package com.datapipeline;

import static com.datapipeline.model.DataTaskHistoricalStat.DP_TASK_HISTORICAL_MAPPING;

import com.datapipeline.core.ElasticSearchClient;
import com.datapipeline.core.ElasticSearchConnect;
import com.datapipeline.model.DataTaskHistoricalStat;
import com.datapipeline.utils.DpUtils;
import com.datapipeline.utils.ObjectConvert;
import com.datapipeline.utils.ParameterTool;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkResponse;
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
        connect.createIndex(
            ElasticSearchTools.indexName,
            DP_TASK_HISTORICAL_MAPPING,
            number_of_shards,
            number_of_replicas);
      }
      multi(connect, parameterTool);
    } else if (Objects.equals(executeMode, "delete")) {
      deleteByQuery(connect, parameterTool);
    } else {
      System.out.println("当前模式不支持:" + executeMode + " ,请重新配置模式");
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
                  batchInsert(connect, mockBase, batchSize, recentlyMonth, rangeIds);
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

  private static void batchInsert(
      ElasticSearchConnect connect, int mockBase, int batchSize, int recentlyMonth, int rangeIds)
      throws Exception {
    // 单位(万条)
    int mockSize = mockBase * 10000;
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
        DataTaskHistoricalStat dataTaskHistoricalStat = generateData(recentlyMonth, rangeIds);
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

  private static DataTaskHistoricalStat generateData(int recentlyMonth, int rangeIds) {
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
    // TODO other for delete
    // 获取当前系统时间 - recentlyMonth个月前之间的时间戳
    long priorTime = DpUtils.getPriorTime(recentlyMonth);
    long randomTime = ThreadLocalRandom.current().nextLong(priorTime, new Date().getTime());
    dataTaskHistoricalStat.setCreatedAt(randomTime);
    return dataTaskHistoricalStat;
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
