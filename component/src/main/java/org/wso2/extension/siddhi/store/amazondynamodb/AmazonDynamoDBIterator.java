/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.store.amazondynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.wso2.siddhi.core.table.record.RecordIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A class representing a RecordIterator which is responsible for processing Amazon DynamoDB Event Table find()
 * operations in a streaming fashion.
 */
public class AmazonDynamoDBIterator implements RecordIterator<Object[]> {

    private QueryResult queryResult;
    private ScanResult scanResult;
    private int index = 0;
    private List<Map<String, AttributeValue>> attributeValues;
    private List<Map<String, AttributeValue>> getAttributeValues;

    public AmazonDynamoDBIterator(QueryResult queryResult, ScanResult scanResult) {
        this.queryResult = queryResult;
        this.scanResult = scanResult;
        if (this.queryResult == null) {
            this.attributeValues = null;
        } else {
            this.attributeValues = queryResult.getItems();
        }
        if (this.scanResult == null) {
            this.getAttributeValues = null;
        } else {
            this.getAttributeValues = scanResult.getItems();
        }
    }

    private static Object getValueFromAttributeValue(AttributeValue value) {
        switch (value) {
            case value.getN() != null:
                return value.getN();
                break;
            case value.getS() != null:
                return value.getS();
                break;
            case value.getBOOL() != null:
                return value.getBOOL();
                break;
            case value.getB() != null:
                return value.getB();
                break;
            default: return null;
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = false;
        if (this.attributeValues == null && this.getAttributeValues != null) {
            if (this.getAttributeValues.size() > index) {
                hasNext = true;
            }
            return hasNext;
        } else if (this.attributeValues != null && this.getAttributeValues == null) {
            if (this.attributeValues.size() > index) {
                hasNext = true;
            }
            return hasNext;
        } else {
            return false;
        }
    }

    @Override
    public Object[] next() {
        if (this.hasNext()) {
            return extractRecord(queryResult, scanResult);
        }
        return new Object[0];
    }

    private Object[] extractRecord(QueryResult queryResult, ScanResult scanResult) {
        List<Object> objects = new ArrayList<>();
        if (queryResult != null) {
            for (AttributeValue attributeValue : queryResult.getItems().get(index++).values()) {
                objects.add(getValueFromAttributeValue(attributeValue));
            }
        } else if (scanResult != null) {
            for (AttributeValue attributeValue : scanResult.getItems().get(index++).values()) {
                objects.add(getValueFromAttributeValue(attributeValue));
            }
        }
        return objects.toArray();
    }
}
