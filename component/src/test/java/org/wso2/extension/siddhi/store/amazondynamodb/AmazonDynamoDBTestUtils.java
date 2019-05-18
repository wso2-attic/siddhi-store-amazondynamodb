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
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;

public class AmazonDynamoDBTestUtils {

    public static final String TABLE_NAME = "AmazonDynamoDBTestTable";
    public static final String TABLE_NAME_ALREADY_EXIST = "Music";
    public static final String SECRET_KEY = "MZLDdDDdfkslfkd;fk;ld";
    public static final String SECRET_KEY_INVALID = "LoooS/xg/PEgVUJ5UW90000000k6nvRYJMXc";
    public static final String ACCESS_KEY = "AKIAY4Qjhkldjslkjf;ls";
    public static final String ACCESS_KEY_INVALID = "AKIAYIONURETJV";
    public static final String SIGNING_REGION = "us-east-1";
    public static final String SORT_KEY = "price";
    public static final String SORT_KEY_NOT_MATCH_WITH_PRIMARYKEY = "volume";
    public static final String SORT_KEY_OF_ALREADY_EXIST_TABLE = "SongTitle";
    public static final String READ_CAPACITY_UNITS = "5";
    public static final String WRITE_CAPACITY_UNITS = "5";
    private static final Log log = LogFactory.getLog(AmazonDynamoDBTestUtils.class);
    private static AmazonDynamoDB client;

    private AmazonDynamoDBTestUtils() {
    }

    public static void init(String tablename) throws AmazonDynamoDBException {
        createClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(TABLE_NAME);
        try {
            table.delete();
            table.waitForDelete();
        } catch (InterruptedException e) {
            log.info(e.getMessage());
        }
    }

    public static boolean checkTable() {
        createClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        boolean tableExist = false;

        Iterator<Table> iterator = tables.iterator();
        while (iterator.hasNext()) {
            Table table = iterator.next();
            if (table.getTableName().equals(TABLE_NAME)) {
                tableExist = true;
            } else {
                tableExist = false;
            }
        }
        return tableExist;
    }

    private static void createClient() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
        client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion(SIGNING_REGION)
                .build();
    }

    public static int getItemCount() {
        createClient();
        new DynamoDB(client);
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(TABLE_NAME);
        ScanResult scanResult = client.scan(scanRequest);
        int count = scanResult.getScannedCount();

        return count;
    }
}
