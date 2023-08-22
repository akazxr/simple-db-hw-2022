/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package simpledb.execution.aggregator.integer;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractAggregator {

    public Map<Field, Integer> resultMap = new HashMap<>();

    public Map<Field, Integer> getResultMap() {
        return resultMap;
    }

    public void setResultMap(Map<Field, Integer> result) {
        this.resultMap = result;
    }

    public abstract void mergeTuple(Field gbFieldInstance, IntField aFieldInstance);
}
