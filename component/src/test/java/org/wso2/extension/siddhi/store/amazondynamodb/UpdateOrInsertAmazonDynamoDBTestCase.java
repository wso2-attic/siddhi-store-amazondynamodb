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
import org.wso2.siddhi.core.stream.input.InputHandler;

import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.ACCESS_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.READ_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SECRET_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SIGNING_REGION;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.WRITE_CAPACITY_UNITS;

public class UpdateOrInsertAmazonDynamoDBTestCase {
    private static final Log log = LogFactory.getLog(DeleteFromAmazonDynamoDBTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== AmasonDynamoDB Table UPDATEORINSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== AmasonDynamoDB Table UPDATEORINSERT tests completed ==");
    }

    @BeforeMethod
    public void init() {
        try {
            AmazonDynamoDBTestUtils.init(TABLE_NAME);
        } catch (AmazonDynamoDBException e) {
            log.error("Test case ignored due to " + e.getMessage(), e);
        }
    }

    @Test(description = "Update or insert successfully with multiple condition")
    public void updateOrInsertWithSingleConditionTest() throws InterruptedException {
        log.info("updateOrInsertWithMultipleConditionTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "   on StockTable.price==price and StockTable.volume==volume ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 95.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 67.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 12.6F, 100L});
        updateStockStream.send(new Object[]{"IBM", 27.6F, 101L});
        Thread.sleep(3000);
        siddhiAppRuntime.shutdown();
        long itemCount = AmazonDynamoDBTestUtils.getItemCount();
        AssertJUnit.assertEquals(3, itemCount);
    }

    @Test(description = "Update or insert successfully with multiple conditions that conatins constants ")
    public void updateOrInsertWithMultipleConditionsTest2() throws InterruptedException, AmazonDynamoDBException {
        log.info("updateOrInsertWithMultipleConditionsTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,currentTime long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long,currentTime long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long, currentTime long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol=='IBM' and StockTable.currentTime==currentTime;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 95.6F, 100L, 1548181800000L});
        updateStockStream.send(new Object[]{"WSO2", 27.6F, 101L, 1548181800000L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        long itemCount = AmazonDynamoDBTestUtils.getItemCount();
        AssertJUnit.assertEquals(1, itemCount);
    }

    @Test(description = "Update or insert successfully with single condition ")
    public void updateOrInsertWithSingleConditionTest3() throws InterruptedException, AmazonDynamoDBException {
        log.info("updateOrInsertWithSingleConditionTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,currentTime long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long,currentTime long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long, currentTime long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.currentTime==currentTime;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 95.6F, 100L, 1548181800000L});
        updateStockStream.send(new Object[]{"WSO2", 27.6F, 101L, 1548181800000L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        long itemCount = AmazonDynamoDBTestUtils.getItemCount();
        AssertJUnit.assertEquals(1, itemCount);
    }

    @Test(description = "Update or insert successfully with two conditions")
    public void updateOrInsertWithTwoConditionsTest4() throws InterruptedException, AmazonDynamoDBException {
        log.info("updateOrInsertWithTwoConditionsTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,currentTime long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long,currentTime long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long, currentTime long); ";
        String query = "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "   on StockTable.volume==volume and StockTable.currentTime==currentTime;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        updateStockStream.send(new Object[]{"GOOG", 12.6F, 100L, 1548181800000L});
        updateStockStream.send(new Object[]{"IBM", 27.6F, 101L, 1548181800000L});
        updateStockStream.send(new Object[]{"WSO2", 27.6F, 101L, 1548181800500L});
        Thread.sleep(3000);
        siddhiAppRuntime.shutdown();
        long itemCount = AmazonDynamoDBTestUtils.getItemCount();
        AssertJUnit.assertEquals(3, itemCount);
    }

    @Test(description = "Update or insert with primary key condition ")
    public void updateOrInsertWithSingleConditionTest5() throws InterruptedException, AmazonDynamoDBException {
        log.info("updateOrInsertWithPrimaryKeyConditionTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long,currentTime long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long,currentTime long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long, currentTime long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 95.6F, 100L, 1548181800000L});
        updateStockStream.send(new Object[]{"WSO2", 27.6F, 101L, 1548181800000L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }
}
