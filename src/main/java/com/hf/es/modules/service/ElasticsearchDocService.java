package com.hf.es.modules.service;

import com.alibaba.fastjson.JSON;
import com.hf.es.entity.vo.ElasticSearchVo;
import com.hf.es.entity.vo.ElasticUpdateVo;
import com.hf.tools.config.enums.ElasticCustomCodeEnum;
import com.hf.tools.config.enums.GlobalCustomCodeEnum;
import com.hf.tools.config.exception.ElasticCustomException;
import com.hf.tools.entity.ResultVo;
import com.hf.tools.util.CommonCustomUtils;
import io.swagger.annotations.ApiOperation;
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
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

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


    public boolean elasticDataSaveNoMsg(Object uuid, Object object) {
        try {
            return elasticDataSave(uuid, object);
        } catch (IOException e) {
            log.error(CommonCustomUtils.LOG_ERROR_OUTPUT_PARAM, uuid, CommonCustomUtils.getStackTraceString(e));
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
    public boolean elasticDataSave(Object uuid, Object object) throws IOException {
        Class<?> clazz = object.getClass();
        String index = elasticsearchIndexService.indexName(clazz.getSimpleName());
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.id(uuid.toString());
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
    public boolean elasticBatchDataSave(Object uuid, List<Object> obs) throws IOException {
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


    public boolean elasticDataUpdateNoMsg(String index, String type, String id, Object object) {
        try {
            return elasticDataUpdate(index, type, id, object);
        } catch (IOException e) {
            log.error(CommonCustomUtils.LOG_ERROR_OUTPUT_PARAM, id, CommonCustomUtils.getStackTraceString(e));
            return false;
        }
    }

    @ApiOperation("es 索引 类型这些已知的情况下使用的方法")
    public boolean elasticDataUpdate(String index, String type, String id, Object object) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.doc(JSON.toJSONString(object), XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.status().equals(RestStatus.OK);
    }

    public List<Object> elasticDataUpdateNoMsg(Object uuid, ElasticUpdateVo vo) {
        try {
            return elasticDataUpdate(uuid, vo);
        } catch (IOException e) {
            log.error(CommonCustomUtils.LOG_ERROR_OUTPUT_PARAM, uuid, CommonCustomUtils.getStackTraceString(e));
            List<Object> list = new ArrayList<>(8);
            list.add("根据条件修改异常");
            return list;
        }
    }


    public List<Object> elasticDataUpdate(Object uuid, ElasticUpdateVo vo) throws IOException {
        log.info("uuid={}, vo={}", uuid, JSON.toJSON(vo));
        String index = vo.getIndex();
        Map<String, Object> searchMap = vo.getSearchMap();
        //设置查询条件
        SearchRequest searchRequest = elasticSearchConfig(index, searchMap, null, null, null);
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        //修改数据多余1条
        if (searchHits.totalHits > 1) {
            return updateDataTooMany(vo, hits);
        }
        SearchHit hit = hits[0];
        if (elasticDataUpdate(index, hit.getType(), hit.getId(), vo.getUpdateColValue())) {
            return Collections.emptyList();
        }
        List<Object> list = new ArrayList<>(hits.length);
        list.add(hit.getSourceAsMap());
        return list;
    }

    @ApiOperation("修改数据多余1条")
    private List<Object> updateDataTooMany(ElasticUpdateVo vo, SearchHit[] hits) throws IOException {
        List<Object> list = new ArrayList<>(hits.length);
        if (vo.isUpdateFlag()) {
            for (SearchHit hit : hits) {
                if (elasticDataUpdate(vo.getIndex(), hit.getType(), hit.getId(), vo.getUpdateColValue())) {
                    continue;
                }
                list.add(hit.getSourceAsMap());
                return list;
            }
            return Collections.emptyList();
        } else {
            for (SearchHit hit : hits) {
                list.add(hit.getSourceAsMap());
            }
            return list;
        }
    }


    public <T> void elasticDataSearch(Object uuid, Class<T> clazz, ElasticSearchVo vo,
                                      ResultVo<List<T>> resultVo) throws IOException {
        String index = vo.getIndex();
        if (StringUtils.isEmpty(index)) {
            index = elasticsearchIndexService.indexName(clazz.getSimpleName());
        }
        Map<String, Object> searchMap = vo.getSearchMap();
        log.info("uuid={}, index={}, searchMap={}", uuid, index, searchMap);
        //设置查询条件
        SearchRequest searchRequest = elasticSearchConfig(index, searchMap, vo.getPageNum(), vo.getPageSize(), vo.getSortColName());
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        if (hits.length > 0) {
            List<T> list = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                list.add(JSON.parseObject(hit.getSourceAsString(), clazz));
            }
            resultVo.setResult(list);
            resultVo.setResultDes(GlobalCustomCodeEnum.SUCCESS.getMsg());
            resultVo.setCode(GlobalCustomCodeEnum.SUCCESS.getCode());
            resultVo.setSuccess(true);
            resultVo.setCount(searchHits.totalHits);
        } else {
            resultVo.setResultDes(ElasticCustomCodeEnum.ES_QUERY_NULL.getMsg());
            resultVo.setCode(ElasticCustomCodeEnum.ES_QUERY_NULL.getCode());
            resultVo.setSuccess(true);
            resultVo.setResult(Collections.emptyList());
        }
    }

    @ApiOperation("查询条件设置")
    private SearchRequest elasticSearchConfig(String index, Map<String, Object> searchMap,
                                              Integer pageNum, Integer pageSize, String sortColName) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(elasticWhereConfig(searchMap));
        //分页
        pageSizeEnabled(searchSourceBuilder, pageNum, pageSize, sortColName);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    @ApiOperation("查询条件设置")
    private BoolQueryBuilder elasticWhereConfig(Map<String, Object> searchMap) {
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        searchMap.keySet().forEach(key -> {
            MatchPhraseQueryBuilder must = QueryBuilders.matchPhraseQuery(key, searchMap.get(key));
            mustQuery.must(must);
        });
        return mustQuery;
    }

    @ApiOperation("分页")
    private void pageSizeEnabled(SearchSourceBuilder searchSourceBuilder, Integer pageNum, Integer pageSize, String sortColName) {
        if (!StringUtils.isEmpty(sortColName)) {
            searchSourceBuilder.sort(sortColName, SortOrder.DESC);
        }

        if (pageNum == null && pageSize == null) {
            return;
        }
        if (StringUtils.isEmpty(sortColName)) {
            throw new ElasticCustomException(ElasticCustomCodeEnum.ES_PAGE_SORT_NOT_EXIST);
        }
        if (pageNum != null && pageSize != null) {
            searchSourceBuilder.sort(sortColName, SortOrder.DESC);
            pageNum = pageNum == 0 ? 0 : pageNum - 1;
            searchSourceBuilder.from(pageNum * pageSize);
            searchSourceBuilder.size(pageSize);
        }
        if (pageNum != null && pageSize == null) {
            searchSourceBuilder.sort(sortColName, SortOrder.DESC);
            pageNum = pageNum == 0 ? 0 : pageNum - 1;
            searchSourceBuilder.from(pageNum * 20);
            searchSourceBuilder.size(20);
        }
        if (pageNum == null) {
            searchSourceBuilder.sort(sortColName, SortOrder.DESC);
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(20);
        }
    }
}
