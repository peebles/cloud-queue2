# Cloud Queue

This library acts as a common abstraction on top of a number of popular cloud queue
implementations.  It provides a simple, common interface on top of SQS, IronMQ, RabbitMQ,
Azure, Rackspace, Kafka and Redis.  This means you can write your application code once, and with only config
file changes use any one of the implementations supported by this library.

## The Cloud Queue Model

The cloud queue behavioral model as far as I understand it, is as follows:  When you dequeue a message,
that message is not removed from the queue; it is marked "invisible" so no other possible dequeuer can
dequeue the same message.  This invisibility marker only lasts a certain amount of time.  If that amount
of time runs out before the message is explicity removed from the queue, it becomes visible again for
others to dequeue.  So when you dequeue, at some point you must explicity remove the message you
dequeued from the queue.

The idea is that we want to deal with the problem that someone dequeues a message, but then dies or otherwise
gets into a bad state before the message is actually handled.  If this happens, the temporarily invisible message
will become visible after a short time, allowing someone else to attempt to process the message.

All of the queue implementations in this library follow this model except for RedisQ.  Redis does not have the
capability to emulate this behavior, at least not in any strait forward way.  So the RedisQ should not be used in
production, or at least in any mission critical situation.  Once a message is dequeued, its gone.  If the process
that is processing the message dies before handling the message, its gone forever.

## Queues and Fifos

There are queues and there are fifos.  SQS is a queue but it is not a fifo.  That is, the order in
which things are enqueued is not the order in which they are dequeued.  SQS is suitable as a work queue,
when you don't really care about the strict ordering of messages.  RackQ, IronMQ and RabbitMQ are strict fifos.
Redis emulates a strict fifo.  I am not sure if Azure is a strict fifo, but I don't think it is.

Kafka is a little different than the other systems listed above.  With Kafka you must define a "keyField"
that is present in every message, like a deviceId or a uuid.  All messages with the same value in their keyField
will be handled in fifo order.  Messages with different keyFields are handled in parallel (out of order).  Also,
if you are running multiple instances of a consumer and all instances shared the same "groupId", then messages
with the same keyField will be handled by the same consumer instance every time.  For these reasons, Kafka
is a great choice for IoT systems.

## Usage - Push Model (consumer)

```javascript
// PRODUCER (enqueue)
let q = require( 'cloud-queue' )( config );
q.producer.connect( function( err ) {
  if ( err ) exit( err );
  q.producer.send( 'myQueueName', { my: "message" }, function( err ) {
    // ...
  });
});

// CONSUMER (dequeue)
let q = require( 'cloud-queue' )( config );
q.consumer.connect( 'myQueueName', function( message, cb ) {
  console.log( JSON.stringify( message ) );
  cb();  // cb() will delete the message from the queue, cb(err) will not.
});
```

## Usage - Pull Model (dequeue)

```javascript
// PRODUCER (enqueue)
let q = require( 'cloud-queue' )( config );
q.producer.connect( function( err ) {
  if ( err ) exit( err );
  q.producer.send( 'myQueueName', { my: "message" }, function( err ) {
    // ...
  });
});

// CONSUMER (dequeue)
let q = require( 'cloud-queue' )( config );
q.consumer.connect( function( err ) {
  if ( err ) exit( err );
  async.forever( function( cb ) {
    q.consumer.dequeue( 'myQueueName', function( err, messages ) {
      if ( err ) return cb( err );
      if ( ! ( messages && messages[0] ) ) {
        console.log( 'no messages available' );
        return cb();
      }
      else {
        console.log( 'dequeued', messages.length, 'messages' );
      }
      async.eachSeries( messages, function( message, cb ) {
        console.log( JSON.stringify( message.msg ) );
        // do some work ,,,
        q.consumer.remove( 'myQueueName', message.handle, function( err ) {
          cb( err );
        });
      }, cb );
    });
  }, function( err ) {
    console.log( err );
    exit( err );
  });
});
```

## Configuration

The object that you pass when creating a cloud queue looks like:

```javascript
{
  class: QUEUE-CLASS-TO-USE,
  logger: OPTIONAL-WINSTON-LIKE-LOGGER-TO-USE,
  retry_times: HOW-MANY-TIMES-TO-RETRY ( default: 6 ),
  connection: { CONNECTION-OPTIONS },
  options: {
    maxNumberOfMessages: 1,  // max number of messages to dequeue at a time
    waitTimeSeconds: 5,      // if no messages, seconds to wait for another poll
    visibilityTimeout: 30    // visibility timeout, iff applicable
  }
}
```

The class names supported as of this writing are; `SQS`, `IronMQ`, `RabbitMQ`, `AzureQ`, `RackQ`,
`KafkaQ` and `RedisQ`.
The `connection` object you pass depends on the class you choose.  See "config-example.json" for
how the configuration should look for each class.

## Consuming 

As you can see from the api, to consume messages you pass a message handler function which
will get called whenever there is a new message.  This handler is passed a callback which you will
call after processing the message.  You won't receive a new message until you call the message
handler callback of the current message.  Still, the rate at which messages are coming in could
be very fast.

Lets say that you are reading from a queue and writing DynamoDB records.  If the write capacity for
your table is 5 (writes per second), then you better keep at or below this value when doing the
DynamoDB writes.  But you may be dequeuing at 100 messages per second!

The solution is to rate limit yourself.  I like this code:

```javascript
let rateLimit = require('function-rate-limit');

let perSec = 5;
let putItem = rateLimit( perSec, 1000, function( table, item, cb ) {
  ddb.putItem( table, item, {}, cb );
});

Q.consumer.connect( table, function( message, cb ) {
  putItem( table, message, function( err ) {
    cb( err );
  });
});
```

Nice and simple.  Will keep your message handling at or below 5 messages per second to match your
table's write capacity.

## Consuming - the "pull" model

Sometimes you just gotta pull.  In this use model, you call the consumer "connect" with no queue name and just
a callback function which is called when the connection has been established.  From then on you must explicitly
"pull" from the queue by calling dequeue().  The dequeue() function will return a list of message items, each
looking like

```javascript
{ msg: QUEUE-MESSAGE,
  handle: OPAQUE-MESSAGE-HANDLE }
```

Deal with the message as you see fit, then call remove() with the queue name and handle to delete the
message from the queue.

## Testing

You can do some testing here.  You can fire up a test environment on your development machine by
executing `docker-compose up -d`.  This will get you a redis service and a rabbitmq service, and
a container called "app" which you can "exec -it" into and run "push-test.js" and "pop-test.js".
You can add kafka to the mix by running "sh ./launch-kafka.sh" and by editing your config.json to
point the connectionString to the ip address of the machine running kafka ($DOCKER_HOST).

The other queuer services require account credentials.
