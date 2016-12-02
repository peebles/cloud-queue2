'use strict';

let retry = require( 'retry-unless' );

module.exports = function( config ) {

  class CloudQueue {

    constructor() {
      if ( config.logger ) {
	this.log = config.logger;
      }
      else {
	this.log = require( 'winston' );
      }

      this.producer = {
	connect: this.producer_connect.bind(this),
	send: this.producer_send.bind(this)
      };

      this.consumer = {
	connect: this.consumer_connect.bind(this),
	length: this.consumer_length.bind(this),
	deleteQueue: this.consumer_deleteQueue.bind(this)
      };
    }

    // subclasses can override
    _shouldStopTrying( err ) {
      return false;
    }

    _try( fcn, cb ) {
      retry({ times: config.retry_times || 6,
	      interval: (retryCount) => {
		return 50 * Math.pow( 2, retryCount );
	      }
      }, (cb) => {
	fcn( cb );
      }, (err) => {
	// return true if we should stop, false if we should continue
	return this._shouldStopTrying( err );
      }, cb );
    }

    producer_connect( cb ) {
      this._producer_connect( cb );
    }

    producer_send( queue, message, cb ) {
      this._try(
	(cb) => {
	  this._enqueue( queue, message, cb );
	},
	cb );
    }
    
    consumer_connect( queue, cb ) {
      this._consumer_connect( queue, cb );
    }

    consumer_length( queue, cb ) {
      this._consumer_length( queue, cb );
    }

    consumer_deleteQueue( queue, cb ) {
      this._consumer_deleteQueue( queue, cb );
    }

    _consumer_length( queue, cb ) {
      // implementation can override if its possible to return the number
      // of messages pending in a queue
      process.nextTick( () => {
	cb( null, 0 );
      });
    }

    _consumer_deleteQueue( queue, cb ) {
      // implementation can override if its possible to delete a queue
      process.nextTick( () => {
	cb();
      });
    }

    _producer_connect( cb ) {
      throw( 'subclasses must override' );
    }

    _consumer_connect( queue, cb ) {
      throw( 'subclasses must override' );
    }

    _enqueue( queue, message, cb ) {
      throw( 'subclasses must override' );
    }
  }

  return CloudQueue;
}
