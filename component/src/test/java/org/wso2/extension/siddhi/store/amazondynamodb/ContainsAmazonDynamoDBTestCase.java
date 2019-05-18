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
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.ACCESS_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.READ_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SECRET_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SIGNING_REGION;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.WRITE_CAPACITY_UNITS;

public class ContainsAmazonDynamoDBTestCase {
    private static final Log log = LogFactory.getLog(DeleteFromAmazonDynamoDBTestCase.class);

    private AtomicInteger eventCount = new AtomicInteger(0);
    private int inEventCount;
    private int waitTime = 500;
    private int timeout = 30000;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {
        log.info("== AmazonDynamoDB Table CONTAINS tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== AmazonDynamoDB Table CONTAINS tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            inEventCount = 0;
            eventArrived = false;
            AmazonDynamoDBTestUtils.init(TABLE_NAME);
        } catch (AmazonDynamoDBException e) {
            log.error("Test case ignored due to :" + e.getMessage(), e);
        }
    }

    @Test(description = "Test contains with one condition.")
    public void containsCheckTest1() throws InterruptedException, AmazonDynamoDBException {
        log.info("containsCheckTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (symbol string,volume long);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
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
                "from FooStream \n" +
                "[(StockTable.symbol == symbol ) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 10L}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 65.6f, 10L});
        stockStream.send(new Object[]{"CSC", 65.6f, 10L});
        fooStream.send(new Object[]{"WSO2", 100L});
        fooStream.send(new Object[]{"IBM", 10L});
        fooStream.send(new Object[]{"WSO22", 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertTrue(eventArrived, "success");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Test contains with two conditions.")
    public void containsCheckTest2() throws InterruptedException, AmazonDynamoDBException {
        log.info("containsCheckTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string,value long);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
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
                "from FooStream \n" +
                "[(StockTable.symbol == name and StockTable.volume == value) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 101L}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 65.6f, 101L});
        stockStream.send(new Object[]{"CSC", 65.6f, 10L});
        fooStream.send(new Object[]{"WSO2", 100L});
        fooStream.send(new Object[]{"IBM", 101L});
        fooStream.send(new Object[]{"WSO22", 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertTrue(eventArrived, "success");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Contains check using a constant with single condition")
    public void containsCheckTest3() throws InterruptedException, AmazonDynamoDBException {
        log.info("containsCheckTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string,value long);\n" +
                "@sink(type='log')" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
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
                "from FooStream \n" +
                "[(StockTable.symbol =='WSO2' ) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 10L}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 65.6f, 10L});
        stockStream.send(new Object[]{"CSC", 65.6f, 10L});
        fooStream.send(new Object[]{"WSO2", 100L});
        fooStream.send(new Object[]{"IBM", 10L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertTrue(eventArrived, "success");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Test contains without primary key condition.")
    public void containsCheckTest4() throws InterruptedException, AmazonDynamoDBException {
        log.info("containsCheckTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (symbol string,volume long);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
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
                "from FooStream \n" +
                "[(StockTable.volume == volume ) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 10L}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 65.6f, 10L});
        stockStream.send(new Object[]{"CSC", 65.6f, 10L});
        fooStream.send(new Object[]{"WSO2", 100L});
        fooStream.send(new Object[]{"IBM", 10L});
        fooStream.send(new Object[]{"WSO22", 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertTrue(eventArrived, "success");
        siddhiAppRuntime.shutdown();
    }
}
