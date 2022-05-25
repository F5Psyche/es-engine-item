package com.hf.config.enums;

/**
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 10:45 2022/5/20
 */
public enum ElasticMetricsEnum {
    /**
     * 平均值
     */
    AVG(1),
    MAX(2),
    MIN(3),
    COUNT(4),
    SUM(5),
    ;

    private int code;

    ElasticMetricsEnum(int code) {
        this.code = code;

    }

    public int getCode() {
        return code;
    }

}
