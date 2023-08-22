/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package simpledb.execution.aggregator.integer;

import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.util.HashMap;
import java.util.Map;

public class AvgAggregator extends AbstractAggregator {

    private Map<Field, Integer> count = new HashMap<>();

    private Map<Field, Integer> sum = new HashMap<>();

    @Override
    public void mergeTuple(Field gbFieldInstance, IntField aFieldInstance) {
        if (count.containsKey(gbFieldInstance)) {
            count.put(gbFieldInstance, count.get(gbFieldInstance) + 1);
        } else {
            count.put(gbFieldInstance, 1);
        }

        if (sum.containsKey(gbFieldInstance)) {
            sum.put(gbFieldInstance, sum.get(gbFieldInstance) + aFieldInstance.getValue());
        } else {
            sum.put(gbFieldInstance, aFieldInstance.getValue());
        }
        int avg = sum.get(gbFieldInstance)/count.get(gbFieldInstance);
        resultMap.put(gbFieldInstance, avg);
    }

}
