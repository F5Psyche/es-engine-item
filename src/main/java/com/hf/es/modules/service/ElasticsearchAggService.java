package com.hf.es.modules.service;

import com.hf.tools.config.enums.ElasticCustomCodeEnum;
import com.hf.tools.config.enums.ElasticMetricsEnum;
import com.hf.tools.config.exception.ElasticCustomException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * 聚合
 *
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 11:57 2022/5/19
 */
@Service
public class ElasticsearchAggService {
    private static Logger log = LoggerFactory.getLogger(ElasticsearchAggService.class);

    private static final String TEMP_FIELD_NAME = "tempFieldName";

    private static final String TEMP_FIELD_NAME_BY = "tempFieldNameBy";

    @Resource
    RestHighLevelClient client;

    /**
     * 查询 field 字段的各项指标
     * select avg(field) from index;
     * select max(field) from index;
     * select min(field) from index;
     * select count(field) from index;
     * select sum(field) from index;
     *
     * @param index       索引
     * @param field       字段
     * @param metricsEnum 指标
     * @return 指标值
     * @throws IOException
     */
    public Object indexFieldStats(String index, String field, ElasticMetricsEnum metricsEnum) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregation = AggregationBuilders.stats(TEMP_FIELD_NAME).field(field);
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Stats stats = searchResponse.getAggregations().get(TEMP_FIELD_NAME);
        if (ElasticMetricsEnum.AVG.equals(metricsEnum)) {
            return stats.getAvg();
        } else if (ElasticMetricsEnum.MAX.equals(metricsEnum)) {
            return stats.getMax();
        } else if (ElasticMetricsEnum.MIN.equals(metricsEnum)) {
            return stats.getMin();
        } else if (ElasticMetricsEnum.COUNT.equals(metricsEnum)) {
            return stats.getCount();
        } else if (ElasticMetricsEnum.SUM.equals(metricsEnum)) {
            return stats.getSum();
        } else {
            throw new ElasticCustomException(ElasticCustomCodeEnum.METRICS_UNKNOWN, metricsEnum);
        }
    }


    /**
     * 根据 fieldBy 分组，查询 field 字段的各项指标
     * select avg(field) from index group by fieldBy;
     *
     * @param index       索引
     * @param field       字段
     * @param fieldBy     分组字段
     * @param metricsEnum 指标
     * @throws IOException
     */
    public void indexFieldStatsByGroup(String index, String field, String fieldBy,
                                       ElasticMetricsEnum metricsEnum) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(TEMP_FIELD_NAME_BY)
                .field(fieldBy);
        termsAggregationBuilder.subAggregation(AggregationBuilders.stats(TEMP_FIELD_NAME).field(field));
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Terms terms = searchResponse.getAggregations().get(TEMP_FIELD_NAME_BY);
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Stats stats = bucket.getAggregations().get(TEMP_FIELD_NAME);
            log.info("avg={}, max={}, min={}, count={}, sum={}", stats.getAvg(), stats.getMax(), stats.getMin(), stats.getCount(), stats.getSum());
        }
    }
}
