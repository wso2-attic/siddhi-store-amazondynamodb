# API Docs - v1.0.0

## Store

### amazondynamodb *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*

<p style="word-wrap: break-word">This extension connects to  amazon dynamoDB store.It also implements read-write operations on connected dynamoDB database.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@Store(type="amazondynamodb", access.key="<STRING>", secret.key="<STRING>", signing.region="<STRING>", table.name="<STRING>", sort.key="<STRING>", read.capacity.units="<LONG>", write.capacity.units="<LONG>")
@PrimaryKey("PRIMARY_KEY")
@Index("INDEX")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">access.key</td>
        <td style="vertical-align: top; word-wrap: break-word">The accessKeyId of the user account to generate the signature</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">secret.key</td>
        <td style="vertical-align: top; word-wrap: break-word">The secret access key</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">signing.region</td>
        <td style="vertical-align: top; word-wrap: break-word">The region of the application access</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name with which the siddhi store  should be persisted in the Amazon DynamoDB database.</td>
        <td style="vertical-align: top">The table name defined in the Siddhi App query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">sort.key</td>
        <td style="vertical-align: top; word-wrap: break-word">The sort key of an item is also known as its range attribute. This range attribute stores items with the same partition key physically close together, in sorted order by the sort key value.</td>
        <td style="vertical-align: top">Sort key should be defined by the user if the table use composite primary key.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">read.capacity.units</td>
        <td style="vertical-align: top; word-wrap: break-word"> The read throughput settings for the table.</td>
        <td style="vertical-align: top">5</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">write.capacity.units</td>
        <td style="vertical-align: top; word-wrap: break-word">The write throughput settings for the table</td>
        <td style="vertical-align: top">5</td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="system-parameters" class="md-typeset" style="display: block; font-weight: bold;">System Parameters</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Parameters</th>
    </tr>
    <tr>
        <td style="vertical-align: top">cache.response.metadata</td>
        <td style="vertical-align: top; word-wrap: break-word">Sets whether to cache response metadata.</td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">client.execution.timeout</td>
        <td style="vertical-align: top; word-wrap: break-word">The timeout for a request (in milliseconds).</td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">Any integer value.</td>
    </tr>
    <tr>
        <td style="vertical-align: top">connection.max.idle.millis</td>
        <td style="vertical-align: top; word-wrap: break-word">The maximum idle time (in milliseconds) for a connection in the connection pool.</td>
        <td style="vertical-align: top">60000</td>
        <td style="vertical-align: top">Any integer value.</td>
    </tr>
    <tr>
        <td style="vertical-align: top">connection.timeout</td>
        <td style="vertical-align: top; word-wrap: break-word">The timeout for creating new connections (in milliseconds).</td>
        <td style="vertical-align: top">10000</td>
        <td style="vertical-align: top">Any integer value.</td>
    </tr>
    <tr>
        <td style="vertical-align: top">connection.ttl</td>
        <td style="vertical-align: top; word-wrap: break-word">The expiration time (in milliseconds) for a connection in the connection pool.By default, it is set to -1, i.e. connections do not expire.</td>
        <td style="vertical-align: top">-1</td>
        <td style="vertical-align: top">Any integer value.</td>
    </tr>
    <tr>
        <td style="vertical-align: top">disable.socket.proxy</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to disable Socket proxies.</td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">max.connections</td>
        <td style="vertical-align: top; word-wrap: break-word">The max connection pool size.</td>
        <td style="vertical-align: top">50</td>
        <td style="vertical-align: top">Any integer value.</td>
    </tr>
    <tr>
        <td style="vertical-align: top">max.consecutive.retries.before.throttling</td>
        <td style="vertical-align: top; word-wrap: break-word">The maximum consecutive retries before throttling.</td>
        <td style="vertical-align: top">100</td>
        <td style="vertical-align: top">Any integer value</td>
    </tr>
    <tr>
        <td style="vertical-align: top">request.timeout</td>
        <td style="vertical-align: top; word-wrap: break-word">The timeout for a request (in milliseconds).</td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">Any integer</td>
    </tr>
    <tr>
        <td style="vertical-align: top">response.metadata.cache.size</td>
        <td style="vertical-align: top; word-wrap: break-word">The response metadata cache size.</td>
        <td style="vertical-align: top">50</td>
        <td style="vertical-align: top">Any integer</td>
    </tr>
    <tr>
        <td style="vertical-align: top">socket.timeout</td>
        <td style="vertical-align: top; word-wrap: break-word">The timeout for reading from a connected socket (in milliseconds).</td>
        <td style="vertical-align: top">50000</td>
        <td style="vertical-align: top">Any integer</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tcp.keep.alive</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to use TCP KeepAlive.</td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">throttle.retries</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to throttle retries.</td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">use.expect.continue</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to utilize the USE_EXPECT_CONTINUE handshake for operations.</td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">use.gzip</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to use gzip compression.</td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">use.reaper</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to use the IdleConnectionReaper to manage stale connections.</td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">validate.after.inactivity.millis</td>
        <td style="vertical-align: top; word-wrap: break-word">The time a connection can be idle in the connection pool before it must be validated that it's still open (in milliseconds).</td>
        <td style="vertical-align: top">5000</td>
        <td style="vertical-align: top">Any integer</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StockStream (symbol string, price float, volume long); @Store(type="amazondynamodb", access.key= "xxxxxxxxx" ,secret.key="aaaaaaaaaa", signing.region="us-east-1", table.name = "Foo", read.capacity.units="10", write.capacity.units="10")
@PrimaryKey("symbol")
define table StockTable (symbol string,volume long,price float) ;
@info (name='query2') from StockStream
select symbol,price,volume
insert into StockTable ;
```
<p style="word-wrap: break-word">This will creates a table in the dynamodb database if it does not exist already with symbol as the partition key which is defined under primaryKey annotation.Then the records are inserted to the table 'Foo' with the fields; symbol,price and volume.Here the non key fields; price and volume are defined during the insertion of records.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream StockStream (symbol string, price float, volume long); @Store(type="amazondynamodb", access.key= "xxxxxxxxx" ,secret.key="aaaaaaaaaa", signing.region="us-east-1", table.name = "Foo", read.capacity.units="10", write.capacity.units="10")
@PrimaryKey("symbol")
define table StockTable (symbol string,volume long,price float) ;
@info (name = 'query2')
from ReadStream#window.length(1) join StockTable on StockTable.symbol==ReadStream.symbols 
select StockTable.symbol as checkName, StockTable.price as checkCategory,
StockTable.volume as checkVolume,StockTable.time as checkTime
insert into OutputStream; 
```
<p style="word-wrap: break-word"> The above example creates a table in the dynamodb database if it does not exist already  with symbol as the partition key which is defined under primaryKey annotation.Then the records are inserted to the table 'Foo' with the fields, symbol, price and volume.Here the non key fields; price and and volume are defined during the insertion of records.Then the table is joined with a stream named 'ReadStream' based on a condition.</p>

