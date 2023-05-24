package com.datapipeline.core;

import com.datapipeline.utils.ParameterTool;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

public class ElasticSearchConnect {
  public static int CONNECT_TIMEOUT_MILLIS = 100_000;
  public static int CONNECTION_REQUEST_TIMEOUT_MILLIS = 50_000;
  public static RestHighLevelClient restHighLevelClient;

  public ElasticSearchConnect(ParameterTool parameterTool) {
    ClassLoader cacheContextProvider = Thread.currentThread().getContextClassLoader();
    Class<NamedXContentProvider> namedXContentProviderClass = NamedXContentProvider.class;
    Thread.currentThread().setContextClassLoader(namedXContentProviderClass.getClassLoader());
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    String user = parameterTool.get("esUser");
    String password = parameterTool.get("esPassword");
    String esNodes = parameterTool.get("esNodes");
    int connectTimeoutMillis = Integer.parseInt(parameterTool.get("connect_timeout_millis"));
    int connectionRequestTimeoutMillis =
        Integer.parseInt(parameterTool.get("connection_request_timeout_millis"));
    int connectionKeepAliveMinutes =
        Integer.parseInt(parameterTool.get("connection_keep_alive_minutes"));
    if (user != null && password != null) {
      credentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(user, password));
    }
    HttpHost[] httpHost = handleEsHosts(esNodes, false);
    RestClientBuilder restClientBuilder =
        RestClient.builder(httpHost)
            .setRequestConfigCallback(
                config -> {
                  config.setConnectTimeout(connectTimeoutMillis);
                  config.setSocketTimeout(connectTimeoutMillis);
                  config.setConnectionRequestTimeout(connectionRequestTimeoutMillis);
                  return config;
                })
            .setHttpClientConfigCallback(
                httpAsyncClientBuilder -> {
                  httpAsyncClientBuilder
                      .setKeepAliveStrategy(
                          (httpResponse, httpContext) ->
                              TimeUnit.MINUTES.toMillis(connectionKeepAliveMinutes))
                      .disableAuthCaching()
                      .setMaxConnTotal(300)
                      .setMaxConnPerRoute(50);
                  return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                });
    restHighLevelClient = new RestHighLevelClient(restClientBuilder);
    Thread.currentThread().setContextClassLoader(cacheContextProvider);
  }

  public RestHighLevelClient getRestHighLevelClient() {
    return restHighLevelClient;
  }

  public synchronized void createIndex(
      String indexName, String mapping, Integer numberOfShards, Integer numberOfReplicas) throws Exception {
    Settings settings =
        Settings.builder()
            .put("number_of_shards", numberOfShards)
            .put("number_of_replicas", numberOfReplicas)
            .build();
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.settings(settings);
    restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    if (mapping != null) {
      PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
      putMappingRequest.source(mapping, XContentType.JSON);
      restHighLevelClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
    }
  }

  // 批量插入 (主要用于造一些测试数据)
  /**
   *注意:
   * 如果原先的 索引mapping文件中没有某个字段，比如字段A，但是传入的content中，有这个字段A，那么最终插入的时候，mapping也会被改为和content中一致的结构
   * 如果原先的mapping结构中有10个字段，但是 content参数中，只构建了8个字段，那么另外两个字段的值将为置为默认值 （比如String类型的数据，会被置为null)
   */
  public BulkResponse bulkSaveDoc(String index, List<Map<String, Object>> content)
      throws IOException {
    BulkRequest bulkRequest = new BulkRequest();
    // 创建index请求 千万注意，这个写在循环外侧，否则UDP协议会有丢数据的情况，看运气
    IndexRequest indexRequest;
    for (Map<String, Object> objectMap : content) {
      indexRequest = new IndexRequest();
      indexRequest.index(index);
      indexRequest.source(objectMap, XContentType.JSON);
      bulkRequest.add(indexRequest);
    }
    System.out.println("es同步数据数量: " + bulkRequest.numberOfActions());
    // 设置索引刷新规则
    bulkRequest.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
    BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    return bulk;
  }

  /**
   *  1,首次更新的时候，如果params中没有某个字段，但是mapping中有这个字段，那么这个字段将被置为默认值 （即首次更新时，ES中索引结构和 mapping中定义的结构保持一致。
   *  2,首次更新的时候，如果params比mapping中多了字段，那么这些字段将会被自动创建，并且以后每次更新也不会消失（即如果再次更新时，params中缺了某些字段，那么这些字段也会被赋予默认值）
   */
  public void upsertDocByPk(String indexName, String mapping, String id, String params)
      throws Exception {
    try {
      UpdateRequest updateRequest = new UpdateRequest();
      updateRequest.index(indexName);
      updateRequest.id(id);
      updateRequest.docAsUpsert(true);
      updateRequest.doc(params, XContentType.JSON);
      restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
    } catch (ElasticsearchStatusException e) {
      if (e.status() == RestStatus.NOT_FOUND) {
        createIndex(indexName, mapping, null, null);
        upsertDocByPk(indexName, mapping, id, params);
      } else {
        throw e;
      }
    }
  }

  public boolean indexExists(String indexName) throws IOException {
    GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
    return restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
  }

  public String getDocByPk(String indexName, String id) throws IOException {
    GetRequest getRequest = new GetRequest();
    getRequest.index(indexName);
    getRequest.id(id);
    GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
    return response.getSourceAsString();
  }

  public static void getDocByTaskId(String indexName, Integer taskId) {

    GetRequest getRequest = new GetRequest();
    getRequest.index(indexName);
  }

  private static HttpHost[] handleEsHosts(String esNodes, boolean isUseHttps) {
    List<HttpHost> list = new ArrayList<>();
    HttpHost[] httpHosts = new HttpHost[] {};
    String[] hostArray = esNodes.split(";");
    for (String host : hostArray) {
      String[] split1 = host.split(":");
      HttpHost httpHost =
          new HttpHost(split1[0], Integer.parseInt(split1[1]), isUseHttps ? "https" : "http");
      list.add(httpHost);
    }
    return list.toArray(httpHosts);
  }

  // 删除
  public BulkByScrollResponse deleteDocByQuery(
      String indexName, QueryBuilder qb, int scrollSize, int slicesSize, int scrollKeepAlive)
      throws IOException {
    DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
    deleteByQueryRequest.indices(indexName);
    deleteByQueryRequest.setQuery(qb);
    deleteByQueryRequest.setBatchSize(scrollSize); // 默认10
    deleteByQueryRequest.setSlices(slicesSize); // 默认分片1,删除并行度
    deleteByQueryRequest.setScroll(TimeValue.timeValueMinutes(scrollKeepAlive)); // 设置滚动查询时间，默认为5分钟
    BulkByScrollResponse bulkByScrollResponse =
        restHighLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    return bulkByScrollResponse;
  }




  public void closeResource() throws IOException {
    if (restHighLevelClient != null) {
      restHighLevelClient.close();
    }
  }
}
