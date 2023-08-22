/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package simpledb.execution.aggregator.string;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;

public class CountAggregator extends AbstractAggregator {

    @Override
    public void mergeTuple(Field gbFieldInstance, StringField aFieldInstance) {
        if (resultMap.containsKey(gbFieldInstance)) {
            resultMap.put(gbFieldInstance, resultMap.get(gbFieldInstance) + 1);
        } else {
            resultMap.put(gbFieldInstance, 1);
        }
    }
}
