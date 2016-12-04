'use strict';

let async = require( 'async' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class RacQ extends CloudQueue {

    constructor() {
      super();

      let defaults = {
	visibilityTimeout: 30,
        waitTimeSeconds: 5,
        maxNumberOfMessages: 1,
      };

      this.options = Object.assign( {}, defaults, config.options );
    }

    _producer_connect( cb ) {
      let Q = require('racq');
      this.pq = new Q( config.connection );
      this.pq.authenticate( config.connection.userName, config.connection.apiKey, (err) => {
	if ( err ) this.log.error( "RackQ: Failed to authenticate:", err );
	cb( err );
      });
    }

    _consumer_connect( cb, messageHandler, rcb ) {
      let Q = require('racq');
      this.cq = new Q( config.connection );
      this.cq.authenticate( config.connection.userName, config.connection.apiKey, (err) => {
	if ( err && rcb ) return rcb( err );
        if ( err && !messageHandler ) return queue( err );
        if ( rcb ) rcb();

	// dequeue mode signature
	if ( ! messageHandler ) return queue();

	async.forever(
          (cb) => {
            this._dequeue( queue, (err,msgs) => {
              if ( err ) {
                this.log.error( err );
                return cb();
              }
              else {
                async.eachSeries( msgs, (message, cb) => {
                  let handle = message.handle;
                  let msg = message.msg;
                  messageHandler( msg, (err) => {
                    if ( err ) return cb( err );
                    this._remove( queue, handle, (err) => {
                      cb( err );
                    });
                  });
                }, (err) => {
                  if ( err ) this.log.error( err );
                  cb();
                });
              }
            });
          },
          (err) => {
            this.log.error( 'not supposed to be here:', err );
          });
	
      });
    }

    _ensureQueue( q, queue, cb ) {
      q.queueExists( queue, (err) => {
	if ( ! err ) return cb( null, true );
	q.createQueue( queue, cb );
      });
    }
    
    _enqueue( queue, message, cb ) {
      this._ensureQueue( this.pq, queue, (err) => {
	if ( err ) return cb( err );
	this.pq.postMessages( queue, message, cb );
      });
    }

    _dequeue( queue, cb ) {
      this._try( (cb) => {
	this._ensureQueue( this.cq, queue, (err) => {
	  if ( err ) return cb( err );
	  let opts = {
	    limit: this.options.maxNumberOfMessages,
	    ttl: this.options.visibilityTimeout,
	    grace: this.options.grace || this.options.visibilityTimeout,
	  };
	  this.cq.claimMessages( queue, opts, (err, messages) => {
	    if ( err ) return cb( err );
	    if ( ! (messages && messages.length) ) {
	      return setTimeout( () => {
		cb( null, [] );
	      }, this.options.waitTimeSeconds * 1000 );
	    }
	    cb( null, messages.map( (m) => {
	      return {
		handle: m,
		msg: m.body
	      };
	    }));
	  });
	});
      }, cb );
    }

    _remove( queue, handle, cb ) {
      this._try( (cb) => {
	this.cq.deleteMessages( queue, handle.id, handle.claimId, cb );
      }, cb );
    }

    _consumer_length( queue, cb ) {
      this._try( (cb) => {
	this.cq.getQueueStats( queue, (err,data) => {
	  // someone who can find access to decent doc, fix this!
	  cb( null, 0 );
	});
      }, cb );
    }

    _consumer_deleteQueue( queue, cb ) {
      this._try( (cb) => {
	this.cq.deleteQueue( queue, cb );
      }, cb );
    }

  }

  return new RacQ();
}
