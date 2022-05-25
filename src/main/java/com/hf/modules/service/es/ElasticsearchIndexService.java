package com.hf.modules.service.es;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.ReflectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 16:55 2022/5/18
 */
@Service
public class ElasticsearchIndexService {

    private static Logger log = LoggerFactory.getLogger(ElasticsearchIndexService.class);

    private static final String INTEGER_TYPE = "Integer";

    private static final String LONG_TYPE = "Long";

    private static final String DATE_TYPE = "Date";

    private static final String STRING_TYPE = "String";


    @Resource
    RestHighLevelClient client;

    /**
     * 创建索引
     * index.number_of_shards 分片数
     * index.number_of_replicas 副本数
     *
     * @param uuid    识别码
     * @param classes 对象集
     * @return 索引是否创建成功
     * @throws IOException
     */
    public boolean elasticIndexCreate(String uuid, Class<?>... classes) throws IOException {
        Map<String, Map<String, String>> map = new HashMap<>(128);
        for (Class<?> clazz : classes) {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                ReflectionUtils.makeAccessible(field);
                if (INTEGER_TYPE.equals(field.getType().getSimpleName())) {
                    map.put(field.getName(), ImmutableMap.of("type", "integer"));
                } else if (DATE_TYPE.equals(field.getType().getSimpleName())) {
                    map.put(field.getName(), ImmutableMap.of("type", "date"));
                } else if (STRING_TYPE.equals(field.getType().getSimpleName())) {
                    map.put(field.getName(), ImmutableMap.of("type", "keyword"));
                } else if (LONG_TYPE.equals(field.getType().getSimpleName())) {
                    map.put(field.getName(), ImmutableMap.of("type", "long"));
                } else {
                    log.info("未封装的类型：uuid={}, colName={}, colType={}", uuid, field.getName(), field.getType().getSimpleName());
                    map.put(field.getName(), ImmutableMap.of("type", "keyword"));
                }
            }
        }

        String index = indexName(classes[0].getSimpleName());
        CreateIndexRequest indexRequest = new CreateIndexRequest(index);
        indexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
        );

        String mapping = JSON.toJSONString(ImmutableMap.of("properties", map));
        log.info("uuid={}, mapping={}", uuid, mapping);
        indexRequest.mapping("_doc", mapping, XContentType.JSON);
        CreateIndexResponse indexResponse = client.indices().create(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    /**
     * 删除索引
     *
     * @param clazz 对象
     * @return
     * @throws IOException
     */
    public boolean elasticIndexDelete(Class<?> clazz) throws IOException {
        String index = indexName(clazz.getSimpleName());
        return elasticIndexDelete(index);
    }

    /**
     * 删除索引
     *
     * @param index 索引
     * @return
     * @throws IOException
     */
    public boolean elasticIndexDelete(String index) throws IOException {
        DeleteIndexRequest indexRequest = new DeleteIndexRequest(index);
        AcknowledgedResponse response = client.indices().delete(indexRequest, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }


    public String indexName(String clazzName) {
        int size = clazzName.length();
        StringBuilder index = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                index.append((clazzName.charAt(i) + "").toLowerCase());
            } else if (Character.isUpperCase(clazzName.charAt(i))) {
                index.append(("-" + clazzName.charAt(i)).toLowerCase());
            } else {
                index.append(clazzName.charAt(i));
            }
        }
        return index.toString();
    }
}
