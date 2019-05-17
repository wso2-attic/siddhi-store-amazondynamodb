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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.ACCESS_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.READ_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SECRET_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SIGNING_REGION;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.WRITE_CAPACITY_UNITS;

public class ReadAmazonDynamoDBTestCase {
    private static final Log log = LogFactory.getLog(ReadAmazonDynamoDBTestCase.class);
    private int removeEventCount;
    private boolean eventArrived;


    @BeforeClass
    public static void startTest() {
        log.info("== AmazonDynamoDB Table READ tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("==AmazonDynamoDB Table READ tests completed ==");
    }

    @BeforeMethod
    public void init() {
        try {
            AmazonDynamoDBTestUtils.init(TABLE_NAME);
        } catch (AmazonDynamoDBException e) {
            log.info("Test case ignored due to :" + e.getMessage());
        }
    }

    @Test(description = "Testing reading with multiple conditions from a amazonDynamoDB table successfully")
    public void readEventAmazonDynamoDBTableTestCase1() throws InterruptedException {

        log.info("readEventsFromAmazonDynamoDBTableTestCase1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (volume long);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkPrice float, checkVolume long);" +
                "\n" +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.symbol=='WSO2' and " +
                "StockTable.volume==FooStream.volume \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkPrice, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 110L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L});
        stockStream.send(new Object[]{"MIT", 97.6f, 100L});
        fooStream.send(new Object[]{110L});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading with one conditions from a amazonDynamoDB table successfully")
    public void readEventAmazonDynamoDBTableTestCase2() throws InterruptedException {

        log.info("readEventsFromAmazonDynamoDBTableTestCase2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkPrice float, checkVolume long);" +
                "\n" +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkPrice, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 110L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L});
        stockStream.send(new Object[]{"MIT", 97.6f, 100L});
        fooStream.send(new Object[]{"WSO2"});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading multiple events without primary key from a amazonDynamoDB table successfully")
    public void readEventAmazonDynamoDBTableTestCase3() throws InterruptedException {

        log.info("readEventsFromAmazonDynamoDBTableTestCase3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (volume long);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkPrice float, checkVolume long);" +
                "\n" +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.volume==FooStream.volume \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkPrice, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L});
        stockStream.send(new Object[]{"MIT", 97.6f, 100L});
        stockStream.send(new Object[]{"III", 97.6f, 100L});
        fooStream.send(new Object[]{100L});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void readEventAmazonDynamoDBTableTestCase4() throws InterruptedException, AmazonDynamoDBException {
        log.info("readEventsFromAmazonDynamoDBDBTableTestCase4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string,vol long);\n" +
                "define stream StockStream (symbol string, price float, volume long,time long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long,checkTime long);" +
                "\n" +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long,time long);\n";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.time(5 sec) join StockTable on (StockTable.symbol==FooStream.name  \n" +
                "or StockTable.volume+50<FooStream.vol)" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume,StockTable.time as checkTime\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 110L, 1548181800001L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L, 1548181800009L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L, 1548181800707L});
        stockStream.send(new Object[]{"MIT", 97.6f, 10L, 1548181800507L});
        fooStream.send(new Object[]{"WSO2", 110});
        fooStream.send(new Object[]{"IBM", 50});
        Thread.sleep(1000);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading with one conditions that contains constant " +
            "from a amazonDynamoDB table successfully")
    public void readEventAmazonDynamoDBTableTestCase5() throws InterruptedException {

        log.info("readEventsFromAmazonDynamoDBTableTestCase5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (symbol string);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkPrice float, checkVolume long);" +
                "\n" +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.symbol=='IBM' " +
                "select StockTable.symbol as checkName, StockTable.price as checkPrice, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 110L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L});
        stockStream.send(new Object[]{"MIT", 97.6f, 100L});
        fooStream.send(new Object[]{"WSO2"});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading with one conditions that contains non primary key condition " +
            "from a amazonDynamoDB table successfully")
    public void readEventAmazonDynamoDBTableTestCase6() throws InterruptedException {

        log.info("readEventsFromAmazonDynamoDBTableTestCase6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (symbol string);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkPrice float, checkVolume long);" +
                "\n" +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.volume==100 " +
                "select StockTable.symbol as checkName, StockTable.price as checkPrice, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;

                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 110L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L});
        stockStream.send(new Object[]{"MIT", 97.6f, 100L});
        fooStream.send(new Object[]{"WSO2"});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }
}
