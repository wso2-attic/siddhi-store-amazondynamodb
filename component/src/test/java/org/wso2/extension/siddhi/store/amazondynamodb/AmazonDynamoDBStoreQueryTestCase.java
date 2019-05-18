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
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.StoreQueryCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.ACCESS_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.READ_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SECRET_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SIGNING_REGION;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.WRITE_CAPACITY_UNITS;

public class AmazonDynamoDBStoreQuery {
    private static final Log log = LogFactory.getLog(AmazonDynamoDBStoreQuery.class);

    @BeforeClass
    public static void startTest() {
        log.info("== AmazonDynamoDB Table STORE QUERY tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("==AmazonDynamoDB Table STORE QUERY tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            AmazonDynamoDBTestUtils.init(TABLE_NAME);
        } catch (AmazonDynamoDBException e) {
            log.error("Test case ignored due to :" + e.getMessage(), e);
        }
    }

    @Test(description = "Testing store query using limit")
    public void storeQueryTest1() throws InterruptedException, AmazonDynamoDBException {
        log.info("storeQueryTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 59.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 AND symbol==\"WSO2\" " +
                "select symbol,volume,price");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol,volume " +
                "limit 2 ");
        EventPrinter.print(events);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select* ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void storeQueryTest2() throws InterruptedException {
        log.info("Test2 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 75 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > volume*3/4  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing store query with selected attributes to be projected")
    public void storeQueryTest3() throws InterruptedException {
        log.info("Test3 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals(2, events[0].getData().length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "select symbol, volume ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals(2, events[0].getData().length);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing store query using order by", expectedExceptions = StoreQueryCreationException.class)
    public void storeQueryTest4() throws InterruptedException {
        log.info("Test4 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price " +
                "order by symbol ");
        EventPrinter.print(events);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void storeQueryTest5() {
        log.info("Test5 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();
            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable " +
                    "on price > 10 " +
                    "select symbol1, volume as totalVolume " +
                    "group by symbol " +
                    "having volume >150");
            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(400L, events[0].getData(1));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void storeQueryTest6() {
        log.info("Test6 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();
            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable1 " +
                    "on price > 10 " +
                    "select symbol, volume as totalVolume " +
                    "group by symbol " +
                    "having volume >150");
            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(400L, events[0].getData(1));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void storeQueryTest7() {
        log.info("Test7 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume,currentTimeMillis() as time\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();
            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable " +
                    "on price > 10 " +
                    "select symbol, volume as totalVolume ,time" +
                    "group by symbol " +
                    "having volume >150");
            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(400L, events[0].getData(1));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(description = "Testing store query with select * condition")
    public void storeQueryTest8() throws InterruptedException {
        log.info("Test8 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select * ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals("WSO2", events[0].getData()[1]);
    }
}
