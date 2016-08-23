var from2 = require('from2');
var Promise = require('promise');
var xtend = require('xtend');

module.exports = function (kinesis, streamName, opts) {
  var streamState = initStreamState(kinesis, streamName, opts);
  var closed = false;

  var readable = from2.obj(function (size, next) {
    if (closed) return next(null, null);
    var promiseChain;

    var promiseChain = (streamState.shards)
      ? getRecords(streamState)
      : getShardList(streamState).then(cycleShards).then(getRecords);

    promiseChain
      .catch(catchCycle(streamState))
      .nodeify(next);
  });

  readable.close = function () {closed = true;};
  readable.streamState = streamState;
  return readable;
};

function catchCycle(streamState) {
  return function (error) {
    return new Promise(function (resolve, reject) {
      // right now, we'll only handle provisioned throughputs errors. Otherwise, we'll emit an error
      if (!error.code || error.code !== 'ProvisionedThroughputExceededException') return reject(error);
      
      return cycleShards(streamState)
        .then(function () { resolve([]); })
    });
  };
}

function cycleShards(streamState) {
  return new Promise(function (resolve, reject) {
    streamState.currShard++;
    
    if (streamState.currShard >= streamState.shards.length) {
      if (!streamState.options.allowLooping) return reject('all shards exhausted');
      streamState.currShard = 0;
    }

    setTimeout(function () {
      resolve(streamState);
    }, streamState.options.cyclepause);
  }).then(getShardIterator);
}

function getRecords(streamState) {
  var shard = streamState.shards[streamState.currShard];
  var iter = streamState.shardIterators[shard];

  return new Promise(function (resolve, reject) {
    streamState.kinesis.getRecords({ShardIterator: iter}, function (err, data) {
      if (err) return reject(err);

      // Update iterator
      streamState.shardIterators[shard] = data.NextShardIterator;
      setTimeout(function () {
        resolve(data.Records);
      }, streamState.options.readpause);
    });
  });
}

function getShardList(streamState, startShard) {
  if (!streamState.shards) streamState.shards = [];

  var options = {StreamName: streamState.streamName};
  if (startShard) options.ExclusiveStartShardId = startShard;

  return new Promise(function (resolve, reject) {
    streamState.kinesis.describeStream(options, function (err, data) {
      if (err) return reject(err);

      data.StreamDescription.Shards.forEach(function (s) {
        streamState.shards.push(s.ShardId);
      });

      if (data.StreamDescription.HasMoreShards) {
        // ask for more shards, starting from the last one we got
        getShardList(streamState, streamState.shards[streamState.shards.length - 1])
          .catch(reject)
          .then(resolve);
      } else {
        resolve(streamState);
      }
    });
  });
}

function getShardIterator(streamState) {
  var shard = streamState.shards[streamState.currShard];

  return new Promise(function (resolve, reject) {
    var params = {
      StreamName: streamState.streamName,
      ShardId: shard,
      ShardIteratorType: streamState.options.iteratorType
    };

    if (params.ShardIteratorType === 'AT_TIMESTAMP') {
      params.Timestamp = streamState.options.iteratorTimestamp || new Date();
    }
    
    streamState.kinesis.getShardIterator(params, function (e, d) {
      if (e) return reject(e);

      streamState.shardIterators[shard] = d.ShardIterator;
      return resolve(streamState);
    });
  });
}

function initStreamState(kinesis, streamName, opts) {
  if (!options) options = {};

  var options = xtend({
    readpause: 1000,
    cyclepause: 1000,
    allowLooping: false,
    iteratorType: 'LATEST'
  }, opts);

  var closed = false;
  var streamState = {
    streamName: streamName,
    shards: null,
    currShard: -1,
    shardIterators: {},
    kinesis: kinesis,
    options: options
  };

  return streamState;
}

module.exports.catchCycle = catchCycle;
module.exports.cycleShards = cycleShards;
module.exports.getShardIterator = getShardIterator;
module.exports.getShardList = getShardList;
module.exports.getRecords = getRecords;
module.exports.cyleShards = cycleShards;
module.exports.initStreamState = initStreamState;
