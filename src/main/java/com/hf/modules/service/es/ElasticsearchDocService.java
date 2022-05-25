package com.hf.modules.service.es;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 11:34 2022/5/20
 */
@Service
public class ElasticsearchDocService {

    private static Logger log = LoggerFactory.getLogger(ElasticsearchDocService.class);

    @Resource
    RestHighLevelClient client;

    @Resource
    ElasticsearchIndexService elasticsearchIndexService;


    public boolean elasticDataSaveNoMsg(String uuid, Object object) {
        try {
            return elasticDataSave(uuid, object);
        } catch (IOException e) {
            log.error("uuid={}, errMsg={}", uuid, e);
            return false;
        }
    }

    /**
     * 存储单条数据到 es
     *
     * @param uuid   识别码
     * @param object 对象
     * @return
     * @throws IOException
     */
    public boolean elasticDataSave(String uuid, Object object) throws IOException {
        Class<?> clazz = object.getClass();
        String index = elasticsearchIndexService.indexName(clazz.getSimpleName());
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.id(uuid);
        indexRequest.type("_doc");
        indexRequest.source(JSON.toJSONString(object), XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.status().equals(RestStatus.CREATED);
    }

    /**
     * 批量存储数据到 es
     *
     * @param uuid 识别码
     * @param obs  对象
     * @return
     * @throws IOException
     */
    public boolean elasticBatchDataSave(String uuid, List<Object> obs) throws IOException {
        log.info("uuid={}, obs={}", uuid, obs);
        Class<?> clazz = obs.get(0).getClass();
        String index = elasticsearchIndexService.indexName(clazz.getSimpleName());
        BulkRequest bulkRequest = new BulkRequest();
        for (Object object : obs) {
            IndexRequest indexRequest = new IndexRequest(index);
            indexRequest.id(UUID.randomUUID().toString());
            indexRequest.type("_doc");
            indexRequest.source(JSON.toJSONString(object), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        return bulkResponse.status().equals(RestStatus.CREATED);
    }

    public boolean elasticDataUpdateNoMsg(String uuid, String index, Object object) {
        try {
            return elasticDataUpdate(uuid, index, object);
        } catch (IOException e) {
            log.error("uuid={}, errMsg={}", uuid, e);
            return false;
        }
    }

    public boolean elasticDataUpdate(String uuid, String index, Object object) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, "_doc", uuid);
        updateRequest.doc(JSON.toJSONString(object), XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.status().equals(RestStatus.OK);
    }


    public <T> List<T> elasticDataSearch(String uuid, Class<T> clazz, Map<String, Object> searchMap) throws IOException {
        String index = elasticsearchIndexService.indexName(clazz.getSimpleName());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        log.info("uuid={}, index={}, searchMap={}", uuid, index, searchMap);
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        searchMap.keySet().forEach(key -> {
            MatchPhraseQueryBuilder must = QueryBuilders.matchPhraseQuery(key, searchMap.get(key));
            mustQuery.must(must);
        });
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(mustQuery);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        if (searchHits.length > 0) {
            List<T> list = new ArrayList<>(searchHits.length);
            for (SearchHit searchHit : searchHits) {
                list.add(JSON.parseObject(searchHit.getSourceAsString(), clazz));
            }
            return list;
        }
        return Collections.emptyList();
    }
}
