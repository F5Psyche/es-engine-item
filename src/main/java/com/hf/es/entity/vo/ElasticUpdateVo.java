package com.hf.es.entity.vo;

import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Map;

/**
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 16:43 2022/6/14
 */
public class ElasticUpdateVo implements Serializable {

    @ApiModelProperty("es 数据索引")
    private String index;

    @ApiModelProperty("修改的字段和内容")
    private Map<String, Object> updateColValue;

    @ApiModelProperty("查询条件")
    private Map<String, Object> searchMap;

    @ApiModelProperty("数据大于一条时，是否确认修改")
    private boolean updateFlag;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Map<String, Object> getUpdateColValue() {
        return updateColValue;
    }

    public void setUpdateColValue(Map<String, Object> updateColValue) {
        this.updateColValue = updateColValue;
    }

    public Map<String, Object> getSearchMap() {
        return searchMap;
    }

    public void setSearchMap(Map<String, Object> searchMap) {
        this.searchMap = searchMap;
    }

    public boolean isUpdateFlag() {
        return updateFlag;
    }

    public void setUpdateFlag(boolean updateFlag) {
        this.updateFlag = updateFlag;
    }
}
