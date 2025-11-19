package com.alibaba.otter.canal.client.adapter.rdb;

/**
 * 同步模式
 * Created by Wuxl at 2025/11/17 15:19
 */
public enum SyncModeEnum {
    /**
     * 普通模式
     */
    NORMAL(0),

    /**
     * 追加模式（UPDATE会转换为INSERT执行）
     */
    INSERT(1);

    private final int code;
    SyncModeEnum(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
