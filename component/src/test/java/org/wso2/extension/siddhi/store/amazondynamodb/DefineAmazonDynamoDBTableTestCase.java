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
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.ACCESS_KEY_INVALID;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.READ_CAPACITY_UNITS;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SECRET_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SECRET_KEY_INVALID;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SIGNING_REGION;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SORT_KEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SORT_KEY_NOT_MATCH_WITH_PRIMARYKEY;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.SORT_KEY_OF_ALREADY_EXIST_TABLE;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.TABLE_NAME_ALREADY_EXIST;
import static org.wso2.extension.siddhi.store.amazondynamodb.AmazonDynamoDBTestUtils.WRITE_CAPACITY_UNITS;

public class DefineAmazonDynamoDBTableTestCase {

    private static final Log log = LogFactory.getLog(DefineAmazonDynamoDBTableTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== AmazonDynamoDB Table DEFINITION tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== AmazonDynamoDB Table DEFINITION tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            AmazonDynamoDBTestUtils.init(TABLE_NAME);
        } catch (AmazonDynamoDBException e) {
            log.error("Test case ignored due to :" + e.getMessage(), e);
        }
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest1", description = "Testing table creation with a simple " +
            "primary key.")
    public void amazonDynamodbTableDefinitionTest1() throws InterruptedException, AmazonDynamoDBException {
        log.info("amazonDynamodbTableDefinitionTest1");
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
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        int itemCount = AmazonDynamoDBTestUtils.getItemCount();
        Assert.assertEquals(3, itemCount);
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest2", description = "Testing table creation with a " +
            "composite primary key.")
    public void amazonDynamodbTableDefinitionTest2() throws InterruptedException, AmazonDynamoDBException {
        log.info("amazonDynamodbTableDefinitionTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", sort.key=\"" + SORT_KEY + "\", " +
                "read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\", \"price\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        int itemCount = AmazonDynamoDBTestUtils.getItemCount();
        Assert.assertEquals(3, itemCount);
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }


    @Test(testName = "amazonDynamodbTableDefinitionTest3", description = "Testing table creation with " +
            "non matching schema.", expectedExceptions = SiddhiAppCreationException.class)
    public void amazonDynamodbTableDefinitionTest3() throws InterruptedException {
        //Testing table creation with non matching schema
        log.info("amazonDynamodbTableDefinitionTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", sort.key=\"" + SORT_KEY_NOT_MATCH_WITH_PRIMARYKEY + "\", " +
                "read.capacity.units=\"" + READ_CAPACITY_UNITS + "\", write.capacity.units=\"" +
                WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\", \"price\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest4", description = "Testing table creation with already " +
            "existing table name.")
    public void amazonDynamodbTableDefinitionTest4() throws InterruptedException, AmazonDynamoDBException {
        //Defining a dynamodb table with already existing table.
        log.info("amazonDynamodbTableDefinitionTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream MusicStream (Artist string, SongTitle string); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME_ALREADY_EXIST + "\", sort.key=\"" + SORT_KEY_OF_ALREADY_EXIST_TABLE + "\", " +
                "read.capacity.units=\"" + READ_CAPACITY_UNITS
                + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"Artist\", \"SongTitle\")" +
                "define table Music (Artist string, SongTitle string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from MusicStream   " +
                "insert into Music ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler musicStream = siddhiAppRuntime.getInputHandler("MusicStream");
        siddhiAppRuntime.start();
        musicStream.send(new Object[]{"WSO2", "abc"});
        musicStream.send(new Object[]{"MALIKI", "mln"});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest5", description = "Testing table creation with " +
            "invalid access key and secret key.")
    public void amazonDynamodbTableDefinitionTest5() throws InterruptedException, AmazonDynamoDBException {
        log.info("amazonDynamodbTableDefinitionTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY_INVALID + "\" ," +
                "secret.key=\"" + SECRET_KEY_INVALID + "\", signing.region=\""
                + SIGNING_REGION + "\", table.name = \"" + TABLE_NAME + "\", read.capacity.units=\""
                + READ_CAPACITY_UNITS + "\", write.capacity.units=\"" + WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest6", description = "Testing table creation " +
            "without primary key field.", expectedExceptions = SiddhiAppCreationException.class)
    public void amazonDynamodbTableDefinitionTest6() throws InterruptedException {
        log.info("amazonDynamodbTableDefinitionTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS + "\", write.capacity.units=\"" +
                WRITE_CAPACITY_UNITS + "\")\n" +
//                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest7", description = "Testing table creation without " +
            "defining the primary key field or sort key.", expectedExceptions = SiddhiAppCreationException.class)
    public void amazonDynamodbTableDefinitionTest7() throws InterruptedException {
        //Testing table creation without defining primary key field or sort key.
        log.info("amazonDynamodbTableDefinitionTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY_INVALID + "\" ," +
                "secret.key=\"" + SECRET_KEY_INVALID + "\", signing.region=\"" +
                SIGNING_REGION + "\", table.name = \"" + TABLE_NAME + "\", read.capacity.units=\"" +
                READ_CAPACITY_UNITS + "\", write.capacity.units=\"" +
                WRITE_CAPACITY_UNITS + "\")\n" +
//                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest8", description = "Testing table creation with invalid " +
            "primary key data types.", expectedExceptions = SiddhiAppCreationException.class)
    public void amazonDynamodbTableDefinitionTest8() throws InterruptedException {
        //Testing table creation with invalid primary key data types.
        log.info("amazonDynamodbTableDefinitionTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol bool, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", table.name = \"" +
                TABLE_NAME + "\", read.capacity.units=\"" + READ_CAPACITY_UNITS + "\", write.capacity.units=\"" +
                WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol bool, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "amazonDynamodbTableDefinitionTest9", description = "Testing table creation without " +
            "defining the table name in the store annotation.")
    public void amazonDynamodbTableDefinitionTest9() throws InterruptedException {
        log.info("amazonDynamodbTableDefinitionTest9");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"amazondynamodb\", access.key= \"" + ACCESS_KEY + "\" ," +
                "secret.key=\"" + SECRET_KEY + "\", signing.region=\"" + SIGNING_REGION + "\", " +
                "read.capacity.units=\"" + READ_CAPACITY_UNITS + "\", write.capacity.units=\"" +
                WRITE_CAPACITY_UNITS + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);
        boolean tableExist = AmazonDynamoDBTestUtils.checkTable();
        AssertJUnit.assertFalse(tableExist);
        siddhiAppRuntime.shutdown();
    }

}
