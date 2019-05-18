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

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.ACCESS_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.READ_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SECRET_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SIGNING_REGION;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SORT_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.WRITE_CAPACITY_UNITS;

public class InsertIntoAmazonDynamoDBTestCase {
    // If you will know about this related testcase,
    // refer https://github.com/wso2-extensions/siddhi-store-rdbms/tree/master/component/src/test
    private static final Log log = LogFactory.getLog(InsertIntoAmazonDynamoDBTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== AmazonDynamoDB Table INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("==AmazonDynamoDB Table INSERT tests completed ==");
    }

    @BeforeMethod
    public void init() {
        try {
            AmazonDynamoDBTestUtils.init(TABLE_NAME);
        } catch (AmazonDynamoDBException e) {
            log.error("Test case ignored due to :" + e.getMessage(), e);
        }
    }

    @Test(description = "Testing insertion with simple primary key")
    public void insertIntoTableTest1() throws InterruptedException, AmazonDynamoDBException {
        log.info("insert items into a table with simple primary key");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        long itemCount = AmazonDynamoDBTestUtils.getItemCount();
        AssertJUnit.assertEquals(2, itemCount);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing insertion with composite primary key")
    public void insertIntoTableKeyTest2() throws InterruptedException, AmazonDynamoDBException {
        log.info("insert items into a table with composite primary key");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", sort.key=\"" + SORT_KEY + "\", read.capacity.units=\""
                + READ_CAPACITY_UNITS + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\", \"price\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        long itemCount = AmazonDynamoDBTestUtils.getItemCount();
        AssertJUnit.assertEquals(2, itemCount);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing insertion with not defined partition key",
            expectedExceptions = SiddhiAppCreationException.class)
    public void insertIntoTableTest3() throws InterruptedException, AmazonDynamoDBException {
        log.info("insert items into a table with not defined partition key");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Table insertion with non matching primary keys",
            expectedExceptions = SiddhiAppCreationException.class)
    public void insertIntoTableTest4() throws InterruptedException, AmazonDynamoDBException {
        log.info("insert Into Table With Non Matching PrimaryKeys");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,time long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", sort.key=\"" + SORT_KEY + "\", read.capacity.units=\""
                + READ_CAPACITY_UNITS + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\", \"volume\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume,time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Insertion with duplicate records", dependsOnMethods = "insertIntoTableTest1")
    public void insertIntoTableWithDuplicateRecordsTest() throws InterruptedException, AmazonDynamoDBException {
        log.info("insert Into Table With Duplicate Records Test");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 110L});
        stockStream.send(new Object[]{"WSO2", 325.6f, 110L});
        long itemCount = AmazonDynamoDBTestUtils.getItemCount();
        AssertJUnit.assertEquals(1, itemCount);
        siddhiAppRuntime.shutdown();
    }

}
