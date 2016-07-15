var test = require('tap').test,
  reader = require('../'),
  AWS = require('aws-sdk'),
  AWSMock = require('aws-sdk-mock');


test('catchCycle retries on throughput exceptions', function (t) {
  AWSMock.mock('Kinesis', 'getShardIterator', function (params, cb) {
    cb(null, {ShardIterator: 'ABCD'});
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1', 'shard-2'];

  reader.catchCycle(streamState)({code: 'ProvisionedThroughputExceededException'}).nodeify(function (err, result) {
    t.error(err);
    t.equal(streamState.shardIterators['shard-1'], 'ABCD', 'shard iterator was updated');
    AWSMock.restore('Kinesis');
    t.end();
  });
});

test('catchCycle fails on other errors', function (t) {
  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1'];

  reader.catchCycle(streamState)({code: 'AnotherException'}).nodeify(function (err, result) {
    t.equal(err.code, 'AnotherException', 'catchCycle rejected with the expected error');
    t.end();
  });
});

test('cycleShards moves to the next shard', function (t) {
  AWSMock.mock('Kinesis', 'getShardIterator', function (params, cb) {
    cb(null, {ShardIterator: 'ABCD'});
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1', 'shard-2'];

  var succeeded = 0;

  function afterCycle(expected) {
    return function () {
      succeeded++;
      t.equal(expected, streamState.currShard, 'cycled to ' + expected + ' shard');
      return streamState;
    }
  }

  reader.cycleShards(streamState) // Initialize onto the first shard
    .then(afterCycle(0))
    .then(reader.cycleShards) // Move to the second shard
    .then(afterCycle(1))
    .then(function () {
      t.equal(succeeded, 2);
      AWSMock.restore('Kinesis');
      t.end();
    });
});

test('cycleShards loops around if allowLooping = true', function (t) {
  AWSMock.mock('Kinesis', 'getShardIterator', function (params, cb) {
    cb(null, {ShardIterator: 'ABCD'});
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {allowLooping: true});
  streamState.shards = ['shard-1', 'shard-2'];

  var succeeded = 0;

  function afterCycle() {
    succeeded++;
    return streamState;
  }

  reader.cycleShards(streamState) // Initialize onto the first shard
    .then(afterCycle)
    .then(reader.cycleShards) // Move to the second shard
    .then(afterCycle)
    .then(reader.cycleShards) // Loop back to the first
    .then(afterCycle)
    .then(function () {
      t.equal(streamState.currShard, 0, 'looped back to the first shard');
      t.equal(succeeded, 3, 'confirm that we cycled 3 times');
      AWSMock.restore('Kinesis');
      t.end();
    });
});

test('cycleShards fails if looping is disabled an no more shards exist', function (t) {
  AWSMock.mock('Kinesis', 'getShardIterator', function (params, cb) {
    cb(null, {ShardIterator: 'ABCD'});
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1', 'shard-2'];

  var succeeded = 0;

  function afterCycle() {
    succeeded++;
    return streamState;
  }

  reader.cycleShards(streamState)
    .then(afterCycle)
    .then(reader.cycleShards)
    .then(afterCycle)
    .then(reader.cycleShards)
    .then(afterCycle)
    .nodeify(function (err, res) {
      t.ok(err, 'received error after cycling past the end of the shard list');
      t.equal(succeeded, 2, 'two successful cycles before failing');
      t.end();
    });
});

test('getRecords resolves with fetched kinesis records', function (t) {
  AWSMock.mock('Kinesis', 'getRecords', function (params, cb) {
    t.equal(params.ShardIterator, 'ABCD');
    cb(null, {
      NextShardIterator: 'EFGH',
      Records: [
        {id: 1},
        {id: 2}
      ]
    });
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1', 'shard-2'];
  streamState.currShard = 0;
  streamState.shardIterators = {'shard-1': 'ABCD'};

  reader.getRecords(streamState).nodeify(function (err, res) {
    t.error(err, 'no error');
    t.deepEqual(res, [{id: 1}, {id: 2}], 'resolved with expected results');
    t.equal(streamState.shardIterators['shard-1'], 'EFGH', 'shard iterator was updated')
    AWSMock.restore('Kinesis');
    t.end();
  });
});

test('getRecords rejects with kinesis errors', function (t) {
  AWSMock.mock('Kinesis', 'getRecords', function (params, cb) {
    t.equal(params.ShardIterator, 'ABCD');
    cb('An error occurred');
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1', 'shard-2'];
  streamState.currShard = 0;
  streamState.shardIterators = {'shard-1': 'ABCD'};

  reader.getRecords(streamState).nodeify(function (err) {
    t.equal(err, 'An error occurred', 'rejected with the expected error');
    AWSMock.restore('Kinesis');
    t.end();
  });
});


test('getShardList sets list of shards for streamState', function (t) {
  AWSMock.mock('Kinesis', 'describeStream', function (params, cb) {
    cb(null, {
      StreamDescription: {
        Shards: [
          {ShardId: 'shard-0'},
          {ShardId: 'shard-1'},
          {ShardId: 'shard-2'},
        ],
        HasMoreShards: false
      }
    });
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});

  reader.getShardList(streamState).nodeify(function (err, res) {
    t.deepEqual(streamState.shards, [
      'shard-0', 
      'shard-1', 
      'shard-2'
    ], 'streamState.shards matches expected');
    AWSMock.restore('Kinesis');
    t.end();
  });
});

test('getShardList loops when there HasMoreShards is true', function (t) {
  var called = 0;
  AWSMock.mock('Kinesis', 'describeStream', function (params, cb) {
    called++;
    if (called == 1) {
      cb(null, {
        StreamDescription: {
          Shards: [
            {ShardId: 'shard-0'},
            {ShardId: 'shard-1'},
            {ShardId: 'shard-2'},
          ],
          HasMoreShards: true
        }
      });
    } else {
      cb(null, {
        StreamDescription: {
          Shards: [
            {ShardId: 'shard-3'},
            {ShardId: 'shard-4'},
            {ShardId: 'shard-5'},
          ],
          HasMoreShards: false
        }
      }); 
    }
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});

  reader.getShardList(streamState).nodeify(function (err, res) {
    t.deepEqual(streamState.shards, [
      'shard-0', 
      'shard-1', 
      'shard-2', 
      'shard-3', 
      'shard-4', 
      'shard-5'
    ], 'streamState.shards matches expected');
    AWSMock.restore('Kinesis');
    t.end();
  });
});

test('getShardList rejects with kinesis errors', function (t) {
  AWSMock.mock('Kinesis', 'describeStream', function (params, cb) {
    cb('An error occurred');
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});

  reader.getShardList(streamState).nodeify(function (err, res) {
    t.equal(err, 'An error occurred');
    AWSMock.restore('Kinesis');
    t.end();
  });
});

test('getShardIterator sets streamState.shardIterators', function (t) {
  AWSMock.mock('Kinesis', 'getShardIterator', function (params, cb) {
    cb(null, {ShardIterator: 'ABCD'});
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1'];
  streamState.currShard = 0;

  reader.getShardIterator(streamState).nodeify(function (err, res) {
    t.error(err);
    t.deepEqual(streamState.shardIterators, {'shard-1': 'ABCD'});
    AWSMock.restore('Kinesis');
    t.end();
  });
});

test('getShardIterator rejects with kinesis errors', function (t) {
  AWSMock.mock('Kinesis', 'getShardIterator', function (params, cb) {
    cb('An error occurred');
  });

  var kinesis = new AWS.Kinesis();
  var streamState = reader.initStreamState(kinesis, 'test-stream', {});
  streamState.shards = ['shard-1'];
  streamState.currShard = 0;

  reader.getShardIterator(streamState).nodeify(function (err, res) {
    t.equal(err, 'An error occurred');
    AWSMock.restore('Kinesis');
    t.end();
  });
});

test('reader sets up and provides a readable stream of kinesis data', function (t) {
  AWSMock.mock('Kinesis', 'describeStream', function (params, cb) {
    cb(null, {
      StreamDescription: {
        Shards: [
          {ShardId: 'shard-0'},
          {ShardId: 'shard-1'},
        ],
        HasMoreShards: false
      }
    });
  });
  AWSMock.mock('Kinesis', 'getShardIterator', function (params, cb) {
    cb(null, {ShardIterator: 'ABCD'});
  });
  AWSMock.mock('Kinesis', 'getRecords', function (params, cb) {
    cb(null, {
      NextShardIterator: 'EFGH',
      Records: [1, 2, 3]
    })
  });

  var kinesis = new AWS.Kinesis();
  var kread = reader(kinesis, 'test-stream');

  var chunks = [];
  kread.on('data', function (records) {
    t.deepEqual(records, [1,2,3], 'receives records from stream');
    kread.close();
  }).on('end', function () {
    t.end();
  });
});