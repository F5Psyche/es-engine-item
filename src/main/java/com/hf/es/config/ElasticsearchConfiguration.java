package com.hf.es.config;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
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

    @Value("${elasticsearch.userName}")
    String userName;

    @Value("${elasticsearch.password}")
    String password;


//    @Bean(destroyMethod = "close", name = "client")
//    public RestHighLevelClient initRestClient() {
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials(userName, password));
//
//        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
//                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
//                        .setConnectTimeout(connTimeout)
//                        .setSocketTimeout(socketTimeout)
//                        .setConnectionRequestTimeout(connectionRequestTimeout)
//                ).setMaxRetryTimeoutMillis(5 * 60 * 1000);
//        return new RestHighLevelClient(builder);
//    }


    @Bean(destroyMethod = "close", name = "client")
    public RestHighLevelClient initRestClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(connTimeout)
                        .setSocketTimeout(socketTimeout)
                        .setConnectionRequestTimeout(connectionRequestTimeout)
                        //没有x-pack可以不用 setHttpClientConfigCallback
                ).setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.disableAuthCaching();
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }).setMaxRetryTimeoutMillis(5 * 60 * 1000);
        return new RestHighLevelClient(builder);
    }
}
