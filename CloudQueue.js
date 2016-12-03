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
	send: this.producer_send.bind(this),
      };

      this.consumer = {
	connect: this.consumer_connect.bind(this),
	length: this.consumer_length.bind(this),
	deleteQueue: this.consumer_deleteQueue.bind(this),
	dequeue: this.consumer_dequeue.bind(this),
	remove: this.consumer_remove.bind(this),
      };
    }

    enqueue( queue, message, cb ) {
      this.producer_send( queue, message, cb );
    }

    dequeue( queue, cb ) {
      this.consumer_dequeue( queue, cb );
    }

    remove( queue, handle, cb ) {
      this.consumer_remove( queue, handle, cb );
    }

    // subclasses can override
    _shouldStopTrying( err ) {
      return false;
    }

    _try( fcn, cb ) {
      retry({ times: config.retry_times || 6,
	      interval: (retryCount) => {
		let ms = 50 * Math.pow( 2, retryCount );
		this.log.warn( 'retrying in', ms );
		return ms;
	      }
      }, (cb) => {
	fcn( cb );
      }, (err) => {
	// return true if we should stop, false if we should continue
	return this._shouldStopTrying( err );
      }, cb );
    }

    producer_connect( cb ) {
      this._try( (cb) => {
	this._producer_connect( cb );
      }, cb );
    }

    producer_send( queue, message, cb ) {
      this._try( (cb) => {
	this._enqueue( queue, message, cb );
      }, cb );
    }
    
    consumer_connect( queue, cb ) {
      if ( ! cb ) {
	// if this is a dequeue model, then we can do retries
	this._try( (cb) => {
	  this._consumer_connect( cb );
	}, queue );
      }
      else {
	// this is a consume model
	this._try( (rcb) => {
	  this._consumer_connect( queue, cb, rcb );
	}, (err) => {
	  if ( err ) throw( err );
	});
      }
    }

    consumer_length( queue, cb ) {
      this._consumer_length( queue, cb );
    }

    consumer_deleteQueue( queue, cb ) {
      this._consumer_deleteQueue( queue, cb );
    }

    consumer_dequeue( queue, cb ) {
      this._dequeue( queue, cb );
    }

    consumer_remove( queue, handle, cb ) {
      this._remove( queue, handle, cb );
    }

    _consumer_length( queue, cb ) {
      // implementation can override if its possible to return the number
      // of messages pending in a queue
      process.nextTick( () => {
	cb( null, 1 );
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

    _dequeue( queue, cb ) {
      throw( 'this consumer does not implement dequeue' );
    }

    _remove( queue, handle, cb ) {
      throw( 'this consumer does not implement remove' );
    }

    _enqueue( queue, message, cb ) {
      throw( 'subclasses must override' );
    }
  }

  return CloudQueue;
}
