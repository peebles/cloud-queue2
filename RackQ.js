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
      this.pq.authenticate( (err) => {
	if ( err ) this.log.error( "RackQ: Failed to authenticate:", err );
	cb( err );
      });
    }

    _consumer_connect( cb ) {
      let Q = require('racq');
      this.cq = new Q( config.connection );
      this.cq.authenticate( (err) => {
	if ( err ) this.log.error( "RackQ: Failed to authenticate:", err );

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

	  let messages = [];
	  async.until(
	    () => { return messages.length; },
	    (cb) => {
	      let opts = {
		limit: this.options.maxNumberOfMessages,
		ttl: this.options.visibilityTimeout,
		grace: this.options.grace || this.options.visibilityTimeout,
	      };



	      this.cq.claimMessages( queue, opts, (err, _messages) => {
		if ( err ) return cb( err );
		messages = _messages;
		if ( ! (messages && messages.length) ) {
		  return setTimeout( () => {
		    cb();
		  }, this.options.waitTimeSeconds * 1000 );
		}
		cb();
	      });
	    },
	    (err) => {
	      if ( err ) return cb( err );
	      cb( null, messages.map( (m) => {
		return {
		  handle: m,
		  msg: m.body
		};
	      }));
	    }
	  );
	});
      }, cb );
    }

    _remove( queue, handle, cb ) {
      this._try( (cb) => {
	this.cq.deleteMessages( queue, handle.id, handle.claimId, cb );
      }, cb );
    }

  }

  return new SQS();
}
