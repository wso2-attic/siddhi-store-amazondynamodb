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

/**
 * Class which holds the constants required by the Amazon DynamoDB Event Table implementation.
 */
public class AmazonDynamoDBTableConstants {

    public static final String ANNOTATION_ELEMENT_ACCESS_KEY = "access.key";
    public static final String ANNOTATION_ELEMENT_SECRET_KEY = "secret.key";
    public static final String ANNOTATION_ELEMENT_SIGNING_REGION = "signing.region";
    public static final String ANNOTATION_ELEMENT_TABLE_NAME = "table.name";
    public static final String ANNOTATION_ELEMENT_SORT_KEY = "sort.key";
    public static final String ANNOTATION_ELEMENT_READ_CAPACITY_UNITS = "read.capacity.units";
    public static final String ANNOTATION_ELEMENT_WRITE_CAPACITY_UNITS = "write.capacity.units";

    //Miscellaneous DYNAMODB constants
    public static final String COMPOSITE_PRIMARY_KEY = "composite";
    public static final String SIMPLE_PRIMARY_KEY = "simple";
    public static final String DYNAMODB_COMPARE_LESS_THAN = "<";
    public static final String DYNAMODB_COMPARE_GREATER_THAN = ">";
    public static final String DYNAMODB_COMPARE_LESS_THAN_EQUAL = "<=";
    public static final String DYNAMODB_COMPARE_GREATER_THAN_EQUAL = ">=";
    public static final String DYNAMODB_COMPARE_EQUAL = "=";
    public static final String DYNAMODB_COMPARE_NOT_EQUAL = "<>";
    public static final String DYNAMODB_AND = "and";
    public static final String DYNAMODB_OR = "or";
    public static final String DYNAMODB_NOT = "not";
    public static final String DYNAMODB_IN = "in";
    public static final String DYNAMODB_IS_NULL = "NULL";
    public static final String DYNAMODB_UPDATE_QUERY = "SET";
    public static final String DYNAMODB_UPDATE_QUERY_AND = ",";
    public static final String WHITESPACE = " ";
    public static final String SEPARATOR = ", ";
    public static final String OPEN_PARENTHESIS = "(";
    public static final String CLOSE_PARENTHESIS = ")";
    public static final String CONSTANT_PLACEHOLDER = ":const";
    public static final String STREAM_VARIABLE_PLACEHOLDER = ":strVar";

    public static final String EXCEPTION_MATH_OPERATOR = "Amazon DynamoDb Event table does not support " +
            "MATH operations.Please check your query and try again.";

    private AmazonDynamoDBTableConstants() {
        //preventing initialization
    }
}
