package com.alibaba.otter.canal.client.adapter.rdb.service;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.support.SingleDml;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by Wuxl at 2023/6/21 16:50
 */
public class RdbSyncServiceHelper {

    public static void convertIfMerged(MappingConfig config, SingleDml dml) {
        Optional.ofNullable(config.getDbMapping().getMergeTag()).ifPresent(mergeTag -> {
            if (mergeTag.equals("child")) {
                // 子表特殊处理：插入转化为更新
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    Map<String, Object> old = new LinkedHashMap<>();
                    Map<String, String> targetPk = config.getDbMapping().getTargetPk();
                    dml.getData().forEach((k, v) -> {
                        if (!targetPk.containsKey(k) && v != null) {
                            old.put(k, null);
                        }
                    });
                    dml.setOld(old);
                    dml.setType("UPDATE");
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    // 子表特殊处理：删除不做任何操作
                    dml.setType(null);
                }
            } else if (mergeTag.equals("root")) {
                // 根表什么都不做
            } else {
                // 什么都不做
            }
        });
    }

    /**
     * 根据同步模式转换语句 <br />
     * 同步模式：0-正常，1-追加（UPDATE、DELETE转INSERT）
     * @param config
     * @param dml
     */
    public static void convertBySyncMode(MappingConfig config, SingleDml dml) {
        // 追加模式：UPDATE、DELETE转INSERT
        if (config.getDbMapping().getSyncMode() == 1
                && (dml.getType().equalsIgnoreCase("UPDATE") || dml.getType().equalsIgnoreCase("DELETE"))) {
            dml.setBeforeType(dml.getType());
            dml.setType("INSERT");
            dml.setBeforeOld(dml.getOld());
            dml.setOld(null);
        }
    }
}
