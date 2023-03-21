package com.datapipeline.core;

import com.datapipeline.utils.ParameterTool;

public class ElasticSearchClient {
  private static volatile ElasticSearchConnect elasticSearchConnect;

  private ElasticSearchClient() {}

  public static ElasticSearchConnect getInstance(ParameterTool parameterTool) {
    if (elasticSearchConnect == null) {
      synchronized (ElasticSearchConnect.class) {
        if (elasticSearchConnect == null) {
          elasticSearchConnect = new ElasticSearchConnect(parameterTool);
        }
      }
    }
    return elasticSearchConnect;
  }
}
