package com.datapipeline.core.query;

import static java.util.Collections.emptyMap;

import com.datapipeline.model.DataTaskDelayMonitorGraph;
import com.datapipeline.utils.DateUtils;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ElasticSearchQuery {

  public static void main(String[] args) {
    System.out.println(LocalDateTime.now());

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.FULL);
    TemporalAccessor parse = dateTimeFormatter.parse("2023-12-03 08:09:23");

    // JDK 1.8之前做法
    System.out.println(TimeZone.getDefault());
    // JDK 1.8之后做法
    System.out.println(ZoneId.systemDefault());
    System.out.println(ZoneId.of("Asia/Shanghai"));
  }

  private RestHighLevelClient restHighLevelClient;

  public ElasticSearchQuery(RestHighLevelClient restHighLevelClient) {
    this.restHighLevelClient = restHighLevelClient;
  }

  public List<DataTaskDelayMonitorGraph> getTaskDelayMonitorGraph(String indexName)
      throws IOException {
    Integer taskId = 1;
    Integer mappingId = 1;
    String start = "2023-03-30 18:00:53";
    String end = "2023-03-31 18:00:53";

    long interval = 3600000;
    long startFormatTime =
        DateUtils.dateStringTransMilli(start)
            - DateUtils.dateStringTransMilli(start) % interval
            + interval;
    long endFormatTime =
        DateUtils.dateStringTransMilli(end)
            - DateUtils.dateStringTransMilli(end) % interval
            + interval;
    BoolQueryBuilder queryBuilder =
        QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("taskId", taskId))
            .must(QueryBuilders.termQuery("mappingId", mappingId))
            .must(QueryBuilders.rangeQuery("createdAt").from(startFormatTime).to(endFormatTime));

    MaxAggregationBuilder batchDelayTimeMax =
        AggregationBuilders.max("batchDelayTimeMax").field("batchDelayTime");
    DateHistogramAggregationBuilder time =
        AggregationBuilders.dateHistogram("time")
            .field("createdAt")
            .fixedInterval(new DateHistogramInterval(interval + "ms"))
            .subAggregation(batchDelayTimeMax);

    SearchRequest searchRequest = new SearchRequest(indexName);
    // 通过SearchSourceBuilder构建搜索参数
    SearchSourceBuilder builder = new SearchSourceBuilder();
    builder.query(queryBuilder).aggregation(time);
    SearchRequest request = searchRequest.source(builder);

    SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
    Aggregations aggregations = response.getAggregations();
    Map<String, Aggregation> queryAggregationMap =
        Optional.ofNullable(response.getAggregations()).map(Aggregations::asMap).orElse(emptyMap());
    ParsedDateHistogram timeHistogram = (ParsedDateHistogram) queryAggregationMap.get("time");
    List<? extends Bucket> buckets = timeHistogram.getBuckets();
    long firstDataCreateTime = ((ZonedDateTime) buckets.get(0).getKey()).toInstant().toEpochMilli();
    long lastDataCreateTime =
        ((ZonedDateTime) buckets.get(buckets.size() - 1).getKey()).toInstant().toEpochMilli();

    List<Long> collect = Stream.iterate(startFormatTime, n -> n + interval).limit((firstDataCreateTime-startFormatTime)/interval).map(var -> {
      return var;
    }).collect(Collectors.toList());

    for (Long aLong : collect) {
      System.out.println("aLong---> "+aLong);
    }
    for (Bucket bucket : buckets) {
      String keyAsString = bucket.getKeyAsString();
      System.out.println(keyAsString);
      Aggregations agg = bucket.getAggregations();
      Map<String, Aggregation> stringAggregationMap = agg.asMap();
      for (Entry<String, Aggregation> stringAggregationEntry : stringAggregationMap.entrySet()) {
        System.out.println(stringAggregationEntry.getKey());
        double value = ((ParsedMax) stringAggregationEntry.getValue()).getValue();
        System.out.println(value);
      }
    }
    restHighLevelClient.close();
    return null;
  }
}
