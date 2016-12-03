'use strict';

let async = require( 'async' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class KafkaQ extends CloudQueue {

    constructor() {
      super();
    }

    _producer_connect( cb ) {
      this.pq = require( 'kafka-queue' )( config.connection );
      this.pq.producer.connect( cb );
    }

    _consumer_connect( queue, messageHandler ) {
      // dequeue mode signature
      if ( ! messageHandler ) throw( new Error( 'Kafka must have a queue and messageHandler on consumer.connect()!' ) );

      this.cq = require( 'kafka-queue' )( config.connection );
      this.cq.consumer.connect( queue, (message, cb) => {
	let handle = message.handle;
	let msg = message.msg;
	messageHandler( msg, (err) => {
	  cb( err );
	});
      });
    }

    _enqueue( queue, message, cb ) {
      this.pq.producer.send( queue, message, cb );
    }

  }

  return new KafkaQ();
}
