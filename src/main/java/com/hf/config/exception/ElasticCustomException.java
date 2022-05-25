package com.hf.config.exception;

import com.hf.config.enums.ElasticCustomCodeEnum;

/**
 * @author zhanghf/f5psyche@163.com
 * @version 1.0
 * @date 15:26 2021/11/4
 */
public class ElasticCustomException extends RuntimeException {

    private final String code;

    private final String resultDes;

    public ElasticCustomException(ElasticCustomCodeEnum codeEnum, Object... cols) {
        super(codeEnum.getMsg());
        this.code = codeEnum.getCode();
        if (cols == null || cols.length == 0) {
            this.resultDes = codeEnum.getMsg();
        } else {
            this.resultDes = String.format(codeEnum.getData(), cols);
        }

    }

    public String getCode() {
        return code;
    }

    public String getResultDes() {
        return resultDes;
    }
}
