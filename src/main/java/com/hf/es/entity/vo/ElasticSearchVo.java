package com.hf.es.entity.vo;

import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Map;

/**
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 16:43 2022/6/14
 */
public class ElasticSearchVo implements Serializable {

    @ApiModelProperty("es 数据索引")
    private String index;

    @ApiModelProperty("es 页码")
    private Integer pageNum;

    @ApiModelProperty("es 每页数量")
    private Integer pageSize;

    @ApiModelProperty("es 排序字段")
    private String sortColName;

    @ApiModelProperty("查询条件")
    private Map<String, Object> searchMap;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Integer getPageNum() {
        return pageNum;
    }

    public void setPageNum(Integer pageNum) {
        this.pageNum = pageNum;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public String getSortColName() {
        return sortColName;
    }

    public void setSortColName(String sortColName) {
        this.sortColName = sortColName;
    }

    public Map<String, Object> getSearchMap() {
        return searchMap;
    }

    public void setSearchMap(Map<String, Object> searchMap) {
        this.searchMap = searchMap;
    }
}
