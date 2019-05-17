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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.SystemParameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.table.record.AbstractQueryableRecordTable;
import org.wso2.siddhi.core.table.record.ExpressionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.collection.operator.CompiledSelection;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.execution.query.selection.OrderByAttribute;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.ANNOTATION_ELEMENT_ACCESS_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.
        ANNOTATION_ELEMENT_READ_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.ANNOTATION_ELEMENT_SECRET_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.
        ANNOTATION_ELEMENT_SIGNING_REGION;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.ANNOTATION_ELEMENT_SORT_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.
        ANNOTATION_ELEMENT_WRITE_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.COMPOSITE_PRIMARY_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.CONSTANT_PLACEHOLDER;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.DYNAMODB_AND;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.DYNAMODB_UPDATE_QUERY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.DYNAMODB_UPDATE_QUERY_AND;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.SIMPLE_PRIMARY_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.STREAM_VARIABLE_PLACEHOLDER;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableConstants.WHITESPACE;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableUtils.checkForValidPrimaryKeyDataTypes;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableUtils.convertToDynamodbDataTypes;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableUtils.initPrimaryKeys;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableUtils.isEmpty;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableUtils.mapValues;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTableUtils.mapValuesToAttributes;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PRIMARY_KEY;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

/**
 * Class representing the Amazon DynamoDB store implementation
 */
@Extension(
        name = "amazondynamodb",
        namespace = "store",
        description = "This extension connects to  amazon dynamoDB store." +
                "It also implements read-write operations on connected dynamoDB database.",
        parameters = {
                @Parameter(
                        name = "access.key",
                        description = "The accessKeyId of the user account to generate the signature",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "secret.key",
                        description = "The secret access key",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "signing.region",
                        description = "The region of the application access",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "table.name",
                        description = "The name with which the siddhi store  should be persisted in the " +
                                "Amazon DynamoDB database.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The table name defined in the Siddhi App query."
                ),
                @Parameter(
                        name = "sort.key",
                        description = "The sort key of an item is also known as its range attribute. " +
                                "This range attribute stores items with the same partition key physically close " +
                                "together, in sorted order by the sort key value.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "Sort key should be defined by the user if the table use composite primary key."

                ),
                @Parameter(
                        name = "read.capacity.units",
                        description = " The read throughput settings for the table.",
                        defaultValue = "5",
                        type = {DataType.LONG}
                ),
                @Parameter(
                        name = "write.capacity.units",
                        description = "The write throughput settings for the table",
                        defaultValue = "5",
                        type = {DataType.LONG}
                )
        },
        systemParameter = {
                @SystemParameter(
                        name = "cache.response.metadata",
                        description = "Sets whether to cache response metadata.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "client.execution.timeout",
                        description = "The timeout for a request (in milliseconds).",
                        defaultValue = "0",
                        possibleParameters = {"Any integer value."}
                ),
                @SystemParameter(
                        name = "connection.max.idle.millis",
                        description = "The maximum idle time (in milliseconds) for a connection in the connection " +
                                "pool.",
                        defaultValue = "60000",
                        possibleParameters = {"Any integer value."}
                ),
                @SystemParameter(
                        name = "connection.timeout",
                        description = "The timeout for creating new connections (in milliseconds).",
                        defaultValue = "10000",
                        possibleParameters = {"Any integer value."}
                ),
                @SystemParameter(
                        name = "connection.ttl",
                        description = "The expiration time (in milliseconds) for a connection in the connection pool." +
                                "By default, it is set to -1, i.e. connections do not expire.",
                        defaultValue = "-1",
                        possibleParameters = {"Any integer value."}
                ),
                @SystemParameter(
                        name = "disable.socket.proxy",
                        description = "Whether to disable Socket proxies.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "max.connections",
                        description = "The max connection pool size.",
                        defaultValue = "50",
                        possibleParameters = {"Any integer value."}
                ),
                @SystemParameter(
                        name = "max.consecutive.retries.before.throttling",
                        description = "The maximum consecutive retries before throttling.",
                        defaultValue = "100",
                        possibleParameters = {"Any integer value"}
                ),
                @SystemParameter(
                        name = "request.timeout",
                        description = "The timeout for a request (in milliseconds).",
                        defaultValue = "0",
                        possibleParameters = {"Any integer"}
                ),
                @SystemParameter(
                        name = "response.metadata.cache.size",
                        description = "The response metadata cache size.",
                        defaultValue = "50",
                        possibleParameters = {"Any integer"}
                ),
                @SystemParameter(
                        name = "socket.timeout",
                        description = "The timeout for reading from a connected socket (in milliseconds).",
                        defaultValue = "50000",
                        possibleParameters = {"Any integer"}
                ),
                @SystemParameter(
                        name = "tcp.keep.alive",
                        description = "Whether to use TCP KeepAlive.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "throttle.retries",
                        description = "Whether to throttle retries.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "use.expect.continue",
                        description = "Whether to utilize the USE_EXPECT_CONTINUE handshake for operations.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "use.gzip",
                        description = "Whether to use gzip compression.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "use.reaper",
                        description = "Whether to use the IdleConnectionReaper to manage stale connections.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "validate.after.inactivity.millis",
                        description = "The time a connection can be idle in the connection pool before it must be " +
                                "validated that it's still open (in milliseconds).",
                        defaultValue = "5000",
                        possibleParameters = {"Any integer"}
                )
        },
        examples = {
                @Example(
                        syntax =
                                "define stream StockStream (symbol string, price float, volume long); " +
                                        "@Store(type=\"amazondynamodb\", access.key= \"xxxxxxxxx\" ," +
                                        "secret.key=\"aaaaaaaaaa\", signing.region=\"us-east-1\", " +
                                        "table.name = \"Foo\", read.capacity.units=\"10\", " +
                                        "write.capacity.units=\"10\")\n" +
                                        "@PrimaryKey(\"symbol\")\n" +
                                        "define table StockTable (symbol string,volume long,price float) ;\n" +
                                        "@info (name='query2') " +
                                        "from StockStream\n" +
                                        "select symbol,price,volume\n" +
                                        "insert into StockTable ;",
                        description = "This will creates a table in the dynamodb database if it does not exist " +
                                "already with symbol as the partition key which is defined under primaryKey " +
                                "annotation.Then the records are inserted to the table 'Foo' with the fields; symbol," +
                                "price and volume.Here the non key fields; price and volume are defined during the " +
                                "insertion of records."
                ),
                @Example(
                        syntax = "define stream StockStream (symbol string, price float, volume long); " +
                                "@Store(type=\"amazondynamodb\", access.key= \"xxxxxxxxx\" ," +
                                "secret.key=\"aaaaaaaaaa\", signing.region=\"us-east-1\", " +
                                "table.name = \"Foo\", read.capacity.units=\"10\", " +
                                "write.capacity.units=\"10\")\n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "define table StockTable (symbol string,volume long,price float) ;\n" +
                                "@info (name = 'query2')\n" +
                                "from ReadStream#window.length(1) join StockTable on " +
                                "StockTable.symbol==ReadStream.symbols \n" +
                                "select StockTable.symbol as checkName, StockTable.price as checkCategory,\n" +
                                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                                "insert into OutputStream; ",

                        description = " The above example creates a table in the dynamodb database if it does not " +
                                "exist already  with symbol as the partition key which is defined under primaryKey " +
                                "annotation.Then the records are inserted to the table 'Foo' with the fields, " +
                                "symbol, price and volume." +
                                "Here the non key fields; price and and volume are defined during the insertion of " +
                                "records.Then the table is joined with a stream named 'ReadStream' based on a " +
                                "condition."
                )
        }

)

/*
 for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#event-table-types
  */
public class AmazonDynamoDBStore extends AbstractQueryableRecordTable {

    private String accessKey;
    private String secretKey;
    private String signingRegion;
    private String tableName;
    private String partitionKey;
    private String sortKey;
    private long readCapacityUnits;
    private long writeCapacityUnits;
    private List<String> attributeNames;
    private AmazonDynamoDB client;
    private DynamoDB dynamoDB;
    private String sortKeyDataType;
    private String partitionKeyDataType;
    private String partitionKeyValue;
    private String sortKeyValue;
    private String primaryKeyType;

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definition of the table with annotations if any
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        Annotation storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        Annotation primaryKeys = AnnotationHelper.getAnnotation(ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        this.accessKey = storeAnnotation.getElement(ANNOTATION_ELEMENT_ACCESS_KEY);
        this.secretKey = storeAnnotation.getElement(ANNOTATION_ELEMENT_SECRET_KEY);
        this.signingRegion = storeAnnotation.getElement(ANNOTATION_ELEMENT_SIGNING_REGION);
        String sortKey = storeAnnotation.getElement(ANNOTATION_ELEMENT_SORT_KEY);
        this.sortKey = isEmpty(sortKey) ? null : sortKey;
        this.readCapacityUnits = Long.parseLong(storeAnnotation.getElement(ANNOTATION_ELEMENT_READ_CAPACITY_UNITS));
        this.writeCapacityUnits = Long.parseLong(storeAnnotation.getElement(ANNOTATION_ELEMENT_WRITE_CAPACITY_UNITS));
        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        this.tableName = isEmpty(tableName) ? tableDefinition.getId() : tableName;
        List<Attribute> schema = tableDefinition.getAttributeList();
        this.attributeNames = tableDefinition.getAttributeList().stream()
                .map(Attribute::getName).collect(Collectors.toList());
        if (isEmpty(this.accessKey)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_ACCESS_KEY + "' for DB " +
                    "connectivity cannot be empty" + " for creating table : " + this.tableName);
        }
        if (isEmpty(this.secretKey)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_SECRET_KEY + "' for DB " +
                    "connectivity cannot be empty" + " for creating table : " + this.tableName);
        }
        if (isEmpty(this.signingRegion)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_SIGNING_REGION +
                    "' for DB connectivity cannot be empty " + " for creating table : " + this.tableName);
        }
        if (isEmpty(Long.toString(this.readCapacityUnits))) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_READ_CAPACITY_UNITS +
                    "'for DB connectivity cannot be empty " + " for creating table : " + this.tableName);
        }
        if (isEmpty(Long.toString(this.writeCapacityUnits))) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_WRITE_CAPACITY_UNITS +
                    "'for DB connectivity cannot be empty " + " for creating table : " + this.tableName);
        }
        if (isEmpty(sortKey)) {
            primaryKeyType = SIMPLE_PRIMARY_KEY;
        } else {
            primaryKeyType = COMPOSITE_PRIMARY_KEY;
        }
        List<Attribute> primaryKeyList = initPrimaryKeys(schema, primaryKeys,
                sortKey, this.tableName);
        primaryKeyList.forEach(attribute -> checkForValidPrimaryKeyDataTypes(attribute.getType()));
        if (primaryKeyType.equals(SIMPLE_PRIMARY_KEY)) {
            this.partitionKey = primaryKeyList.get(0).getName();
            this.partitionKeyDataType = convertToDynamodbDataTypes(primaryKeyList.get(0).getType());
        } else {
            for (Attribute attribute : primaryKeyList) {
                if (attribute.getName().equals(sortKey)) {
                    this.sortKeyDataType = convertToDynamodbDataTypes(attribute.getType());
                } else {
                    this.partitionKey = attribute.getName();
                    this.partitionKeyDataType = convertToDynamodbDataTypes(attribute.getType());
                }
            }
        }
    }

    /**
     * Add records to the Table
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     */
    @Override
    protected void add(List<Object[]> records) {
        try {
            Map<String, AttributeValue> attributesValueMap = new HashMap<>();
            for (Object[] record : records) {
                attributesValueMap = mapValuesToAttributes(record,
                        this.attributeNames);
            }
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(this.tableName)
                    .withItem(attributesValueMap);
            client.putItem(putItemRequest);
        } catch (AmazonDynamoDBException e) {
            throw new AmazonDynamoDBTableException("Error while inserting records to " + this.tableName + " : "
                    + e.getMessage());
        }
    }

    /**
     * Find records matching the compiled condition
     *
     * @param findConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                  compiled condition
     * @param compiledCondition         the compiledCondition against which records should be matched
     * @return RecordIterator of matching records
     */
    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) {
        AmazonDynamoDBCompiledCondition amazonDynamoDBCompiledCondition = (AmazonDynamoDBCompiledCondition)
                compiledCondition;
        String query = amazonDynamoDBCompiledCondition.getCompiledQuery();
        Map<Integer, Object> variableMap = amazonDynamoDBCompiledCondition.getParameters();
        Map<Integer, Object> constantMap = amazonDynamoDBCompiledCondition.getParametersConst();
        Map<Integer, Object> conditionParamMap = new HashMap<>();
        Map<Integer, Object> paramMap = new HashMap<>();
        String[] conditionArray = query.split(DYNAMODB_AND);
        int i = 1;
        for (Map.Entry<String, Object> map : findConditionParameterMap.entrySet()) {
            conditionParamMap.put(i, map.getValue());
            i++;
        }
        Stack<Object> parameterValueMap = getParameterValueMap(conditionParamMap, query, conditionArray,
                paramMap, constantMap);
        List<Object> list = new ArrayList<>(variableMap.values());
        List<String> attributeExpression = list.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        Map<String, AttributeValue> valueMap = mapValuesToAttributes(parameterValueMap.toArray(), attributeExpression);
        ScanResult scanResult = null;
        QueryResult queryResult = null;
        try {
            if (!query.contains(partitionKey)) {
                scanResult = queryWithoutPrimaryKey(null, valueMap, null, query);
            } else {
                queryResult = queryWithPrimaryKey(amazonDynamoDBCompiledCondition, valueMap, null, null);
            }
            return new AmazonDynamoDBIterator(queryResult, scanResult);

        } catch (AmazonDynamoDBException e) {
            throw new AmazonDynamoDBTableException("Error retrieving records from table : '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    /**
     * This method will be used to query data without specifying primary key in the amazon dynamodb table.
     */
    private ScanResult queryWithoutPrimaryKey(String selectors, Map<String, AttributeValue> valueMap, Integer limit,
                                              String query) {
        ScanRequest scanRequest;
        if (!query.equals(":const0")) {
            scanRequest = new ScanRequest()
                    .withTableName(this.tableName)
                    .withFilterExpression(query)
                    .withProjectionExpression(selectors)
                    .withExpressionAttributeValues(valueMap)
                    .withLimit(limit);
        } else {
            scanRequest = new ScanRequest()
                    .withTableName(this.tableName)
                    .withProjectionExpression(selectors)
                    .withLimit(limit);
        }
        return client.scan(scanRequest);
    }

    /**
     * This method will be used to query data with specifying primary key in the amazon dynamodb table.
     */
    private QueryResult queryWithPrimaryKey(AmazonDynamoDBCompiledCondition amazonDynamoDBCompiledCondition,
                                            Map<String, AttributeValue> valueMap,
                                            String selectors, Integer limit) {
        String keyCondition = Objects.requireNonNull(amazonDynamoDBCompiledCondition.getKeyCondition().toString());
        StringBuilder nonKeyCondition = amazonDynamoDBCompiledCondition.getNonKeyCondition();
        QueryRequest queryRequest;
        if (nonKeyCondition == null) {
            queryRequest = new QueryRequest()
                    .withTableName(this.tableName)
                    .withProjectionExpression(selectors)
                    .withKeyConditionExpression(keyCondition)
                    .withFilterExpression(null)
                    .withLimit(limit)
                    .withExpressionAttributeValues(valueMap);
        } else {
            queryRequest = new QueryRequest()
                    .withTableName(this.tableName)
                    .withProjectionExpression(selectors)
                    .withKeyConditionExpression(keyCondition)
                    .withFilterExpression(Objects.requireNonNull(nonKeyCondition.toString()))
                    .withLimit(limit)
                    .withExpressionAttributeValues(valueMap);
        }
        return client.query(queryRequest);
    }

    /**
     * Check if matching record exist or not
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     */
    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) {
        AmazonDynamoDBCompiledCondition amazonDynamoDBCompiledCondition =
                (AmazonDynamoDBCompiledCondition) compiledCondition;
        String query = amazonDynamoDBCompiledCondition.getCompiledQuery();
        Map<Integer, Object> variableMap = amazonDynamoDBCompiledCondition.getParameters();
        Map<Integer, Object> constantMap = amazonDynamoDBCompiledCondition.getParametersConst();
        Map<Integer, Object> conditionParamMap = new HashMap<>();
        Map<Integer, Object> paramMap = new HashMap<>();
        String[] conditionArray = query.split(DYNAMODB_AND);
        int i = 1;
        for (Map.Entry<String, Object> map : containsConditionParameterMap.entrySet()) {
            conditionParamMap.put(i, map.getValue());
            i++;
        }
        Stack<Object> parameterValueMap = getParameterValueMap(conditionParamMap, query, conditionArray,
                paramMap, constantMap);
        List<Object> list = new ArrayList<>(variableMap.values());
        List<String> attributeExpression = list.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        Map<String, AttributeValue> valueMap = mapValuesToAttributes(parameterValueMap.toArray(), attributeExpression);
        ScanResult scanResult;
        QueryResult queryResult;
        try {
            if (!query.contains(partitionKey)) {
                scanResult = queryWithoutPrimaryKey(null, valueMap, null, query);
                return scanResult.getScannedCount() != 0;
            } else {
                queryResult = queryWithPrimaryKey(amazonDynamoDBCompiledCondition, valueMap, null, null);
                return queryResult.getScannedCount() != 0;
            }
        } catch (AmazonDynamoDBException e) {
            throw new AmazonDynamoDBException("Error performing 'contains'  on the table : " + this.tableName + ": "
                    + e.getMessage());
        }
    }

    /**
     * Delete all matching records
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     **/
    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) {
        AmazonDynamoDBCompiledCondition amazonDynamoDBCompiledCondition
                = (AmazonDynamoDBCompiledCondition) compiledCondition;
        String condition = amazonDynamoDBCompiledCondition.getCompiledQuery();
        Map<Integer, Object> variableMap = amazonDynamoDBCompiledCondition.getParameters();
        Map<Integer, Object> constantMap = amazonDynamoDBCompiledCondition.getParametersConst();
        Map<Integer, Object> conditionParamMap = new HashMap<>();
        Map<String, Object> valueMap = new HashMap<>();
        Map<Integer, Object> list = new HashMap<>();
        String[] conditionArray = condition.split(DYNAMODB_AND);
        Stack<Object> variablesMap = new Stack<>();
        int i = 1;
        for (Map<String, Object> map : deleteConditionParameterMaps) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                conditionParamMap.put(i, entry.getValue());
                i++;
            }
        }
        Stack<Object> parameterValueMap =
                getParameterValueMap(conditionParamMap, condition, conditionArray, list, constantMap);
        for (Map.Entry<Integer, Object> val : variableMap.entrySet()) {
            variablesMap.push(val.getValue());
        }
        getPrimaryKeyValues(condition, conditionArray, list);
        Iterator<Object> iterator2 = variablesMap.iterator();
        Iterator<Object> iterator1 = parameterValueMap.iterator();

        while (iterator2.hasNext() && iterator1.hasNext()) {
            valueMap.put(variablesMap.pop().toString(), parameterValueMap.pop());

        }
        Table table = dynamoDB.getTable(this.tableName);
        try {
            if (!condition.equals(":const0")) {
                DeleteItemSpec deleteItemSpec;
                if (primaryKeyType.equals(COMPOSITE_PRIMARY_KEY)) {
                    deleteItemSpec = new DeleteItemSpec()
                            .withPrimaryKey(new PrimaryKey(partitionKey, partitionKeyValue, sortKey, sortKeyValue))
                            .withConditionExpression(condition)
                            .withValueMap(valueMap);
                } else {
                    deleteItemSpec = new DeleteItemSpec()
                            .withPrimaryKey(new PrimaryKey(partitionKey, partitionKeyValue))
                            .withConditionExpression(condition)
                            .withValueMap(valueMap);
                }
                table.deleteItem(deleteItemSpec);
            }
        } catch (AmazonDynamoDBException e) {
            throw new AmazonDynamoDBException("Error deleting records from table: " + this.tableName + " : "
                    + e.getMessage());
        }
    }

    /**
     * This method will be used to get parameter variables value map for the delete, update, contains,
     * find and query operations of the amazon dynamodb store.
     */
    private Stack<Object> getParameterValueMap(Map<Integer, Object> conditionParamMap, String condition,
                                               String[] conditionArray, Map<Integer, Object> paramMap,
                                               Map<Integer, Object> constantMap) {
        Stack<Object> parameterValues = new Stack<>();
        int countConst = 1;
        int countVar = 1;
        int count = 1;
        if (condition.contains(DYNAMODB_AND)) {
            for (String con : conditionArray) {
                if (con.contains(CONSTANT_PLACEHOLDER)) {
                    parameterValues.push(constantMap.get(countConst));
                    paramMap.put(count, constantMap.get(countConst));
                    countConst++;
                    count++;
                } else if (con.contains(STREAM_VARIABLE_PLACEHOLDER)) {
                    parameterValues.push(conditionParamMap.get(countVar));
                    paramMap.put(count, conditionParamMap.get(countVar));
                    countVar++;
                    count++;
                }
            }
        } else {
            if (condition.contains(CONSTANT_PLACEHOLDER)) {
                parameterValues.push(constantMap.get(countConst));
                paramMap.put(count, constantMap.get(countConst));
            } else if (condition.contains(STREAM_VARIABLE_PLACEHOLDER)) {
                parameterValues.push(conditionParamMap.get(countVar));
                paramMap.put(count, conditionParamMap.get(countVar));
            }
        }
        return parameterValues;
    }

    /**
     * This method will be used to separate sort key value and partition key value from the parameter value map
     * that is used in delete operation.
     */
    private void getPrimaryKeyValues(String condition, String[] conditionArray,
                                     Map<Integer, Object> paramMap) {
        int index = 1;
        if (primaryKeyType.equals(COMPOSITE_PRIMARY_KEY)) {
            try {
                if (condition.contains(DYNAMODB_AND)) {
                    for (String con : conditionArray) {
                        if (con.contains(partitionKey)) {
                            partitionKeyValue = paramMap.get(index).toString();
                        } else if (con.contains(sortKey)) {
                            sortKeyValue = paramMap.get(index).toString();
                        } else {
                            index++;
                        }
                    }
                }
            } catch (AmazonDynamoDBException err) {
                throw new AmazonDynamoDBException("Unable to query the table without specifying the primary key " +
                        "values of the table " + this.tableName + err.getMessage());
            }
        } else {
            try {
                if (condition.contains(DYNAMODB_AND)) {
                    for (String con : conditionArray) {
                        if (con.contains(this.partitionKey)) {
                            partitionKeyValue = paramMap.get(index).toString();
                        } else {
                            index++;
                        }
                    }
                } else if (condition.contains(this.partitionKey)) {
                    partitionKeyValue = paramMap.get(index).toString();
                }
            } catch (AmazonDynamoDBException err) {
                throw new AmazonDynamoDBException("Unable to query the table without specifying the primary key " +
                        "values of the table " + this.tableName + err.getMessage());
            }
        }
    }

    /**
     * Update all matching records
     *
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the attributes and values that should be updated for the matching records
     */
    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> list1) {
        StringBuilder updateCondition = new StringBuilder();
        AmazonDynamoDBCompiledCondition amazonDynamoDBCompiledCondition
                = (AmazonDynamoDBCompiledCondition) compiledCondition;
        String condition = amazonDynamoDBCompiledCondition.getCompiledQuery();
        updateCondition.append(DYNAMODB_UPDATE_QUERY).append(WHITESPACE);
        for (Map.Entry<String, CompiledExpression> expression : map.entrySet()) {
            if (!expression.getKey().equals(partitionKey)) {
                updateCondition.append(expression.getKey()).append("=").append(":")
                        .append(expression.getKey()).append(",");
            }
        }
        String conditionToUpdate = updateCondition.toString().substring(0, updateCondition.length() - 1);
        Map<Integer, Object> variableMap = amazonDynamoDBCompiledCondition.getParameters();
        Map<String, Object> valueMap = new HashMap<>();
        Stack<Object> variablesMap = new Stack<>();
        Map<Integer, Object> constantMap = amazonDynamoDBCompiledCondition.getParametersConst();
        Map<Integer, Object> conditionParamMap = new HashMap<>();
        Map<Integer, Object> mapParam = new HashMap<>();
        String[] conditionArray = condition.split(DYNAMODB_AND);
        int i = 1;
        for (Map<String, Object> map1 : list) {
            for (Map.Entry<String, Object> entry : map1.entrySet()) {
                conditionParamMap.put(i, entry.getValue());
                i++;
            }
        }
        Stack<Object> parameterValueMap =
                getParameterValueMap(conditionParamMap, condition, conditionArray, mapParam, constantMap);
        ArrayList<Integer> keys = new ArrayList<>(variableMap.keySet());
        for (int j = keys.size() - 1; j >= 0; j--) {
            variablesMap.push(variableMap.get(keys.get(j)));
        }
        for (Map<String, Object> record : list1) {
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                if (!entry.getKey().equals(partitionKey)) {
                    Object obj = ":" + entry.getKey();
                    variablesMap.push(obj);
                    parameterValueMap.push(entry.getValue());

                }
            }
        }
        getPrimaryKeyValues(condition, conditionArray, mapParam);
        Iterator<Object> iterator2 = variablesMap.iterator();
        Iterator<Object> iterator1 = parameterValueMap.iterator();
        while (iterator2.hasNext() && iterator1.hasNext()) {
            valueMap.put(variablesMap.pop().toString(), parameterValueMap.pop());
        }
        Table table = dynamoDB.getTable(this.tableName);
        try {
            UpdateItemSpec updateItemSpec;
            if (primaryKeyType.equals(COMPOSITE_PRIMARY_KEY)) {
                updateItemSpec = new UpdateItemSpec()
                        .withPrimaryKey(partitionKey, partitionKeyValue, sortKey, sortKeyValue)
                        .withUpdateExpression(conditionToUpdate)
                        .withConditionExpression(condition)
                        .withValueMap(valueMap);
            } else {
                updateItemSpec = new UpdateItemSpec()
                        .withPrimaryKey(partitionKey, partitionKeyValue)
                        .withUpdateExpression(conditionToUpdate)
                        .withConditionExpression(condition)
                        .withValueMap(valueMap);
            }
            table.updateItem(updateItemSpec);
        } catch (AmazonDynamoDBException e) {
            throw new AmazonDynamoDBTableException("Error when updating records in th table : " + this.tableName + " : "
                    + e.getMessage());
        }
    }

    /**
     * Try updating the records if they exist else add the records
     *
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the values for adding new records if the update condition did not match
     * @param list2             the attributes and values that should be updated for the matching records
     */
    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> list1,
                               List<Object[]> list2) {
        StringBuilder updateCondition = new StringBuilder();
        AmazonDynamoDBCompiledCondition amazonDynamoDBCompiledCondition = (AmazonDynamoDBCompiledCondition)
                compiledCondition;
        String condition = amazonDynamoDBCompiledCondition.getCompiledQuery();
        updateCondition.append(DYNAMODB_UPDATE_QUERY).append(WHITESPACE)
                .append(condition.replace(DYNAMODB_AND, DYNAMODB_UPDATE_QUERY_AND));
        Map<Integer, Object> variableMap = amazonDynamoDBCompiledCondition.getParameters();
        Map<String, Object> valueMap = new HashMap<>();
        Stack<Object> variablesMap = new Stack<>();
        Stack<Object> parameterValueMap = new Stack<>();
        Map<String, Object> attributesValueMap;
        for (Object[] record : list2) {
            attributesValueMap = mapValues(record, attributeNames);
            for (Map.Entry<String, Object> map1 : attributesValueMap.entrySet()) {
                if (map1.getKey().equals(partitionKey)) {
                    partitionKeyValue = (String) map1.getValue();
                } else if (map1.getKey().equals(sortKey)) {
                    sortKeyValue = (String) map1.getValue();
                } else {
                    parameterValueMap.push(map1.getValue());
                }
            }
            ArrayList<Integer> keys = new ArrayList<>(variableMap.keySet());
            for (int i = keys.size() - 1; i >= 0; i--) {
                variablesMap.push(variableMap.get(keys.get(i)));
            }
            Iterator<Object> iterator2 = variablesMap.iterator();
            Iterator<Object> iterator1 = parameterValueMap.iterator();
            while (iterator2.hasNext() && iterator1.hasNext()) {
                valueMap.put(variablesMap.pop().toString(), parameterValueMap.pop());
            }
            Table table = dynamoDB.getTable(this.tableName);
            UpdateItemSpec updateItemSpec;
            try {
                if (primaryKeyType.equals(SIMPLE_PRIMARY_KEY)) {
                    updateItemSpec = new UpdateItemSpec()
                            .withPrimaryKey(partitionKey, partitionKeyValue)
                            .withUpdateExpression(updateCondition.toString())
                            .withValueMap(valueMap);
                } else {
                    updateItemSpec = new UpdateItemSpec()
                            .withPrimaryKey(partitionKey, partitionKeyValue, sortKey, sortKeyValue)
                            .withUpdateExpression(updateCondition.toString())
                            .withValueMap(valueMap);
                }
                table.updateItem(updateItemSpec);
            } catch (AmazonDynamoDBException e) {
                throw new AmazonDynamoDBException("Error in updating or inserting records to table " + this.tableName
                        + " : " + e.getMessage());
            }
        }
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        AmazonDynamoDBConditionVisitor visitor = new AmazonDynamoDBConditionVisitor(this.partitionKey,
                this.sortKey, this.primaryKeyType);
        expressionBuilder.build(visitor);
        return new AmazonDynamoDBCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.getParametersConst(), visitor.getKeyCondition(), visitor.getNonKeyCondition());
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void connect() throws ConnectionUnavailableException {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(signingRegion)
                .build();
        dynamoDB = new DynamoDB(client);
        checkAndCreateTable();
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect.
     */
    @Override
    protected void disconnect() {
        dynamoDB.shutdown();
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    protected void destroy() {
    }

    private Long resolveQuery(AmazonDynamoDBCompiledSelection amazonDynamoDBCompiledSelection,
                              StringBuilder orderByQuery) {
        Long limit = null;
        AmazonDynamoDBCompiledCondition compiledGroupByClause
                = amazonDynamoDBCompiledSelection.getCompiledGroupByClause();
        if (compiledGroupByClause != null) {
            throw new AmazonDynamoDBTableException("Group by is defined in the query for " + this.tableName +
                    " But it is not defined for amazondynamodb store implementation  ");
        }
        if (amazonDynamoDBCompiledSelection.getLimit() != null) {
            limit = amazonDynamoDBCompiledSelection.getLimit();
        }
        AmazonDynamoDBCompiledCondition compiledOrderBy = amazonDynamoDBCompiledSelection.getCompiledOrderByClause();
        if (compiledOrderBy != null) {
            if (primaryKeyType.equals(COMPOSITE_PRIMARY_KEY)) {
                String orderByClause = compiledOrderBy.getCompiledQuery();
                if (orderByClause.contains(sortKey)) {
                    orderByQuery.append(orderByClause);
                } else {
                    throw new AmazonDynamoDBTableException("Order by is defined in the query for " + this.tableName +
                            "Items with the same partition key value are sorted by sort key in " +
                            "amazondynamodb store implementation. But in the query order by is defined on non " +
                            "sort key attribute.");
                }
            } else {
                throw new AmazonDynamoDBTableException("Order by is defined in the query for " + this.tableName +
                        "Items with the same partition key value are sorted by sort key in " +
                        "amazondynamodb store implementation. But since " + this.tableName + " compose of simple " +
                        "primary key, sort keys has not defined.");
            }
        }
        AmazonDynamoDBCompiledCondition compiledHavingClause
                = amazonDynamoDBCompiledSelection.getCompiledHavingClause();
        if (compiledHavingClause != null) {
            throw new AmazonDynamoDBTableException("Having is defined in the query for " + this.tableName +
                    " But it is not defined for amazondynamodb store implementation  ");
        }
        return limit;
    }


    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] outputAttributes) {
        AmazonDynamoDBCompiledSelection amazonDynamoDBCompiledSelection
                = (AmazonDynamoDBCompiledSelection) compiledSelection;
        AmazonDynamoDBCompiledCondition amazonDynamoDBCompiledCondition
                = (AmazonDynamoDBCompiledCondition) compiledCondition;
        String selectors = amazonDynamoDBCompiledSelection.getCompiledSelectClause().getCompiledQuery();
        String query = amazonDynamoDBCompiledCondition.getCompiledQuery();
        StringBuilder orderByClause = new StringBuilder();

        Map<Integer, Object> constantMap = amazonDynamoDBCompiledCondition.getParametersConst();
        Long limit = resolveQuery(amazonDynamoDBCompiledSelection, orderByClause);
        Integer intLimit;
        Object[] record = constantMap.values().toArray();
        if (limit != null) {
            intLimit = limit.intValue();
        } else {
            intLimit = null;
        }
        Map<Integer, Object> variableMap = amazonDynamoDBCompiledCondition.getParameters();
        Map<String, AttributeValue> valueMap;
        List<Object> list = new ArrayList<>(variableMap.values());
        List<String> attributeExpression = list.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        valueMap = mapValuesToAttributes(record, attributeExpression);
        QueryResult queryResult = null;
        ScanResult scanResult = null;
        try {
            if (!query.contains(partitionKey)) {
                scanResult = queryWithoutPrimaryKey(selectors, valueMap, intLimit, query);
            } else {
                queryResult = queryWithPrimaryKey(amazonDynamoDBCompiledCondition, valueMap, selectors, intLimit);
            }
        } catch (AmazonDynamoDBException e) {
            throw new AmazonDynamoDBTableException("Error when executing query: " + query + " in the store "
                    + this.tableName + "' : " + e.getMessage(), e);
        }
        return new AmazonDynamoDBIterator(queryResult, scanResult);
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders,
                                                 Long limit, Long offset) {
        return new AmazonDynamoDBCompiledSelection(
                compileSelectClause(selectAttributeBuilders),
                (groupByExpressionBuilder == null) ? null : compileClause(groupByExpressionBuilder),
                (havingExpressionBuilder == null) ? null :
                        compileClause(Collections.singletonList(havingExpressionBuilder)),
                (orderByAttributeBuilders == null) ? null : compileOrderByClause(orderByAttributeBuilders),
                limit);
    }

    private AmazonDynamoDBCompiledCondition compileSelectClause(List<SelectAttributeBuilder> selectAttributeBuilders) {
        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;
        for (SelectAttributeBuilder selectAttributeBuilder : selectAttributeBuilders) {
            AmazonDynamoDBConditionVisitor visitor = new AmazonDynamoDBConditionVisitor(this.partitionKey,
                    this.sortKey, this.primaryKeyType);
            selectAttributeBuilder.getExpressionBuilder().build(visitor);

            String compiledCondition = visitor.returnCondition();

            compiledSelectionList.append(compiledCondition);
            compiledSelectionList.append(SEPARATOR);
            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 1;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;
        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }
        return new AmazonDynamoDBCompiledCondition(compiledSelectionList.toString(), paramMap,
                null, null, null);
    }

    private AmazonDynamoDBCompiledCondition compileClause(List<ExpressionBuilder> expressionBuilders) {
        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;
        for (ExpressionBuilder expressionBuilder : expressionBuilders) {
            AmazonDynamoDBConditionVisitor visitor = new AmazonDynamoDBConditionVisitor(this.partitionKey,
                    this.sortKey, this.primaryKeyType);
            expressionBuilder.build(visitor);

            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition).append(SEPARATOR);

            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;
        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }
        return new AmazonDynamoDBCompiledCondition(compiledSelectionList.toString(), paramMap,
                null, null, null);
    }

    private AmazonDynamoDBCompiledCondition compileOrderByClause
            (List<OrderByAttributeBuilder> orderByAttributeBuilders) {
        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        for (OrderByAttributeBuilder orderByAttributeBuilder : orderByAttributeBuilders) {
            AmazonDynamoDBConditionVisitor visitor = new AmazonDynamoDBConditionVisitor(this.partitionKey,
                    this.sortKey, this.primaryKeyType);
            orderByAttributeBuilder.getExpressionBuilder().build(visitor);

            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition);
            OrderByAttribute.Order order = orderByAttributeBuilder.getOrder();
            if (order == null) {
                compiledSelectionList.append(SEPARATOR);
            }
        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 2); // Removing the last comma separator.
        }
        return new AmazonDynamoDBCompiledCondition(compiledSelectionList.toString(), paramMap,
                null, null, null);
    }

    /**
     * This will check whether the table is exist
     */
    private boolean checkTable() {
        boolean tableExist = false;
        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        for (Table table : tables) {
            if (table.getTableName().equals(this.tableName)) {
                tableExist = true;
                break;
            }
        }
        return tableExist;
    }

    /**
     * This will check whether the table is exist, if not will create the table
     */
    private void checkAndCreateTable() throws ConnectionUnavailableException {
        try {
            if (!checkTable()) {
                List<KeySchemaElement> elements = new ArrayList<>();
                List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
                if (primaryKeyType.equals(COMPOSITE_PRIMARY_KEY)) {
                    KeySchemaElement hashKey = new KeySchemaElement()
                            .withKeyType(KeyType.HASH)
                            .withAttributeName(partitionKey);
                    KeySchemaElement rangeKey = new KeySchemaElement()
                            .withKeyType(KeyType.RANGE)
                            .withAttributeName(sortKey);
                    elements.add(hashKey);
                    elements.add(rangeKey);
                    attributeDefinitions.add(new AttributeDefinition()
                            .withAttributeName(partitionKey)
                            .withAttributeType(partitionKeyDataType));
                    attributeDefinitions.add(new AttributeDefinition()
                            .withAttributeName(sortKey)
                            .withAttributeType(sortKeyDataType));
                } else if (primaryKeyType.equals(SIMPLE_PRIMARY_KEY)) {
                    KeySchemaElement hashKey = new KeySchemaElement()
                            .withKeyType(KeyType.HASH)
                            .withAttributeName(partitionKey);
                    elements.add(hashKey);
                    attributeDefinitions.add(new AttributeDefinition()
                            .withAttributeName(partitionKey)
                            .withAttributeType(partitionKeyDataType));
                }
                CreateTableRequest createTableRequest = new CreateTableRequest()
                        .withTableName(this.tableName)
                        .withKeySchema(elements)
                        .withProvisionedThroughput(new ProvisionedThroughput()
                                .withReadCapacityUnits(readCapacityUnits)
                                .withWriteCapacityUnits(writeCapacityUnits))
                        .withAttributeDefinitions(attributeDefinitions);
                Table table = dynamoDB.createTable(createTableRequest);
                table.waitForActive();
            }
        } catch (AmazonDynamoDBTableException | InterruptedException e) {
            throw new ConnectionUnavailableException("Failed to create table " + this.tableName + " due to : "
                    + e.getMessage());
        }
    }
}
