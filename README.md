## kinesis-cycling-readable

![](https://api.travis-ci.org/tcql/kinesis-cycling-readable.svg)

### Usage 

```js
var kinesisRead = require('kinesis-cycling-readable');

var stream = kinesisRead(
  new AWS.Kinesis({region: 'us-east-1'}),
  'stream-name'
});

stream.on('data', function (records) {
});
```

Upon encountering an `ProvisionedThroughputExceededException`, the stream will automatically cycle to the next shard in the stream and keep reading.

#### Options

```js
{
  readpause: 1000, // Milliseconds to wait between `getRecords` calls
  cyclepause: 1000, // Milliseconds to wait after cycling shards
  allowLooping: false, // If true, when the last shard is cycled, start back at the first shard
                       // If false, the stream emits an error when the last shard is exhausted
  iteratorType: 'LATEST', // Type of ShardIterator to request. Must be one of 
                          // TRIM_HORIZON | LATEST | AT_TIMESTAMP
  iteratorTimestamp: null // Timestamp to use for `AT_TIMESTAMP` ShardIterators
}
```
