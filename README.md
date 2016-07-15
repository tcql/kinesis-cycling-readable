## kinesis-cycling-readable

### Usage 

```js
var kinesisRead = require('kinesis-cycling-readable');

var stream = kinesisRead(
  new AWS.Kinesis({region: 'us-east-1'}),
  'stream-name',
  'LATEST'
});

stream.on('data', function (records) {
});
```

Upon encountering an `ProvisionedThroughputExceededException`, the stream will automatically cycle to the next shard in the stream and keep reading.