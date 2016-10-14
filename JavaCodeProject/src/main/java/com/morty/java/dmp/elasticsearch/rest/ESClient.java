package com.morty.java.dmp.elasticsearch.rest;


import org.apache.commons.httpclient.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Created by morty on 2016/09/28.
 */
public class ESClient {

    RestClient restClient;
    HttpHost[] httpHosts;

    public void init() {

        restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http")
        ).build();


    }

    public void ClientWithConfig() {
        // TODO: 2016/09/28  相关客户端配置
        /**
         * 1、setDefaultHeaders
         * 2、setMaxRetryTimeoutMillis
         * 3、setFailureListener
         * 4、setRequestConfigCallback
         * 5、setHttpClientConfigCallback
         */
        restClient = RestClient.builder(new HttpHost("localhost", 9200))
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setConnectTimeout(5000)
                                .setSocketTimeout(30000);
                    }
                })
                .setMaxRetryTimeoutMillis(30000)
                .build();
    }

    public void ClientPerformRequest(RestClient restClient, String method, String endpoint,

                                     Map<String, String> params,
                                     HttpEntity entity,
                                     ResponseListener responseListener,
                                     HttpAsyncResponseConsumer<HttpResponse> responseConsumer,
                                     Header... headers) {
        /**
         * // Synchronous variants
         Response performRequest(String method, String endpoint,
         Header... headers)
         throws IOException;

         Response performRequest(String method, String endpoint,
         Map<String, String> params, Header... headers)
         throws IOException;

         Response performRequest(String method, String endpoint,
         Map<String, String> params,
         HttpEntity entity,
         Header... headers)
         throws IOException;

         Response performRequest(String method, String endpoint,
         Map<String, String> params,
         HttpEntity entity,
         HttpAsyncResponseConsumer<HttpResponse> responseConsumer,
         Header... headers)
         throws IOException;

         // Asynchronous variants
         void performRequestAsync(String method, String endpoint,
         ResponseListener responseListener,
         Header... headers);

         void performRequestAsync(String method, String endpoint,
         Map<String, String> params,
         ResponseListener responseListener,
         Header... headers);

         void performRequestAsync(String method, String endpoint,
         Map<String, String> params,
         HttpEntity entity,
         ResponseListener responseListener,
         Header... headers);

         void performRequestAsync(String method, String endpoint,
         Map<String, String> params,
         HttpEntity entity,
         ResponseListener responseListener,
         HttpAsyncResponseConsumer<HttpResponse> responseConsumer,
         Header... headers);
         */
        restClient.performRequestAsync(method, endpoint, responseListener, (org.apache.http.Header[]) headers);

        //restClient.performRequest()
    }


    public org.elasticsearch.client.Response ClientResponse(RestClient restClient) {
        // TODO: 2016/09/28 返回结果
        /**
         * Response response = restClient.performRequest("GET", "/",
         Collections.singletonMap("pretty", "true"));
         System.out.println(EntityUtils.toString(response.getEntity()));

         //index a document
         HttpEntity entity = new NStringEntity(
         "{\n" +
         "    \"user\" : \"kimchy\",\n" +
         "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
         "    \"message\" : \"trying out Elasticsearch\"\n" +
         "}", ContentType.APPLICATION_JSON);
         Response indexResponse = restClient.performRequest(
         "PUT",
         "/twitter/tweet/1",
         Collections.<String, String>emptyMap(),
         entity);
         */
        return null;
    }

    public void close() {
        try {
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
