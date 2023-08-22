/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package simpledb.execution.aggregator.integer;

import simpledb.storage.Field;
import simpledb.storage.IntField;

public class SumAggregator extends AbstractAggregator {

    @Override
    public void mergeTuple(Field gbFieldInstance, IntField aFieldInstance) {
        if (resultMap.containsKey(gbFieldInstance)) {
            resultMap.put(gbFieldInstance, resultMap.get(gbFieldInstance) + aFieldInstance.getValue());
        } else {
            resultMap.put(gbFieldInstance, aFieldInstance.getValue());
        }
    }

}
