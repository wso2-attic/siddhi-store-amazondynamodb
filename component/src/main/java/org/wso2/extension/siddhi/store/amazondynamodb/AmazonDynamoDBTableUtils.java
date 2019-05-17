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

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class which holds the utility methods which are used by various units in the Amazon Dynamodb Event Table
 * implementation.
 */

public class AmazonDynamoDBTableUtils {

    private AmazonDynamoDBTableUtils() {
        //preventing initialization
    }

    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    /**
     * This method is used to extract the primary keys provided by the user.
     *
     * @param schema      schema of the table defined by the user.
     * @param primaryKeys primary keys defined by the user
     * @return List of primary keys.
     */
    public static List<Attribute> initPrimaryKeys(List<Attribute> schema, Annotation primaryKeys, String sortKey,
                                                  String tableName) {
        List<String> keys = new ArrayList<>();
        for (Element element : primaryKeys.getElements()) {
            keys.add(element.getValue());
        }
        List<Attribute> primaryKeyList = new ArrayList<>();
        schema.forEach(attribute -> keys.stream()
                .filter(key -> key.equals(attribute.getName().trim()))
                .forEach(key -> primaryKeyList.add(attribute)));
        if (sortKey != null) {
            if (primaryKeyList.isEmpty()) {
                throw new AmazonDynamoDBException("Primary key annotation contains a value which is not present " +
                        "in the attributes of the event table.");
            }
            if (primaryKeyList.size() > 2) {
                throw new SiddhiAppCreationException("Composite primary key consist of two attributes.But schema has " +
                        "defined more than two attributes for the table " + tableName);
            } else {
                boolean containSortKey = false;
                for (Attribute attribute : primaryKeyList) {
                    if (attribute.getName().contains(sortKey)) {
                        containSortKey = true;
                    }
                }
                if (!containSortKey) {
                    throw new SiddhiAppCreationException("Sort key provided by the user does not match with the " +
                            "schema of the table " + tableName);
                }
            }
        } else {
            if (primaryKeyList.isEmpty()) {
                throw new AmazonDynamoDBException("Primary key annotation contains a value which is not present " +
                        "in the attributes of the event table.");
            }
            if (primaryKeyList.size() > 1) {
                throw new SiddhiAppCreationException("Simple primary key consist of one attribute.But schema has " +
                        "defined more than one attributes for the table " + tableName);
            }
        }
        return primaryKeyList;
    }

    /**
     * This method converts Siddhi data types to Amazon Dynamodb data types.
     *
     * @param siddhiDataType siddhi data type of the attribute
     * @return returns the relevant String to the relevant dynamodb data type
     */
    public static String convertToDynamodbDataTypes(Attribute.Type siddhiDataType) {
        String dynamodbDataType;
        switch (siddhiDataType) {
            case STRING:
                dynamodbDataType = "S";
                break;
            case INT:
                dynamodbDataType = "N";
                break;
            case LONG:
                dynamodbDataType = "N";
                break;
            case DOUBLE:
                dynamodbDataType = "N";
                break;
            case FLOAT:
                dynamodbDataType = "N";
                break;
            case BOOL:
                dynamodbDataType = "BOOL";
                break;
            case OBJECT:
                dynamodbDataType = "B";
                break;
            default:
                dynamodbDataType = "";
        }
        return dynamodbDataType;
    }

    /**
     * This method is used to check primary key data types are valid.
     */
    public static void checkForValidPrimaryKeyDataTypes(Attribute.Type dataType) {
        boolean validDataType = true;
        if (dataType == Attribute.Type.BOOL) {
            validDataType = false;
        }
        if (!validDataType) {
            throw new SiddhiAppCreationException("Invalid primary key data type for the table.Must be a string," +
                    " number or binary");
        }
    }

    public static Map<String, AttributeValue> mapValuesToAttributes(Object[] record, List<String> attributeNames) {
        Map<String, AttributeValue> attributesValueMap = new HashMap<>();
        for (int i = 0; i < record.length; i++) {
            attributesValueMap.put(attributeNames.get(i), ItemUtils.toAttributeValue(record[i]));
        }
        return attributesValueMap;
    }

    public static Map<String, Object> mapValues(Object[] record, List<String> attributeNames) {
        Map<String, Object> attributesValueMap = new HashMap<>();
        for (int i = 0; i < record.length; i++) {
            attributesValueMap.put(attributeNames.get(i), record[i]);
        }
        return attributesValueMap;
    }
}
