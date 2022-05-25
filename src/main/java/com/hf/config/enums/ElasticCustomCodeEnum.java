package com.hf.config.enums;

/**
 * 全局自定义错误编码枚举
 *
 * @author zhanghf
 * @version 1.0
 * @date 10:35 2020/3/11
 */
public enum ElasticCustomCodeEnum {
    /**
     * 交易请求成功
     */
    SUCCESS("ES00", "成功!", "成功"),

    METRICS_UNKNOWN("ES01", "指标不存在", "指标'%s'无法处理"),

    UNKNOWN_ERROR("ES99", "未知异常", "异常：%s"),
    ;

    private String code;
    private String msg;
    private String data;


    ElasticCustomCodeEnum(String code, String msg, String data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public String getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public String getData() {
        return data;
    }
}
