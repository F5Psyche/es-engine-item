package com.hf.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * es 搜索引擎
 *
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 14:50 2022/5/18
 */
@Configuration
public class ElasticsearchConfiguration {

    @Value("${elasticsearch.host}")
    String host;

    @Value("${elasticsearch.port}")
    int port;

    @Value("${elasticsearch.connTimeout}")
    int connTimeout;

    @Value("${elasticsearch.socketTimeout}")
    int socketTimeout;

    @Value("${elasticsearch.connectionRequestTimeout}")
    int connectionRequestTimeout;


    @Bean(destroyMethod = "close", name = "client")
    public RestHighLevelClient initRestClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(connTimeout)
                        .setSocketTimeout(socketTimeout)
                        .setConnectionRequestTimeout(connectionRequestTimeout));
        return new RestHighLevelClient(builder);
    }
}
