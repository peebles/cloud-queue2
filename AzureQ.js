'use strict';

let async = require( 'async' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class AzureQ extends CloudQueue {

    constructor() {
      super();

      let defaults = {
	visibilityTimeout: 30,
        waitTimeSeconds: 5,
        maxNumberOfMessages: 1,
      };
      this.options = Object.assign( {}, defaults, config.options );
    }

    _showStopTrying( err ) {
      return err.message.match( /The lock supplied is invalid/ );
    }

    _producer_connect( cb ) {
      try {
        this.pq = require( 'azure-sb' ).createServiceBusService(
          config.connection.connectionString
        );
	process.nextTick( cb );
      } catch( err ) {
	process.nextTick( function() {
	  cb( err );
	});
      }
    }

    _consumer_connect( queue, messageHandler ) {
      try {
        this.cq = require( 'azure-sb' ).createServiceBusService(
          config.connection.connectionString
        );

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
	
      } catch( err ) {
	this.log.error( err );
      }
    }
    
    _enqueue( queue, message, cb ) {
      this.pq.createQueueIfNotExists( queue, this.options, (err) => {
	if ( err ) return cb( err );
	this.pq.sendQueueMessage( queue, { body: JSON.stringify( message ) }, ( err ) => {
	  if ( err ) return cb( err );
	  cb();
	});
      });
    }

    _dequeue( queue, cb ) {
      this._try( (cb) => {
	this.cq.createQueueIfNotExists( queue, this.options, (err) => {
	  if ( err ) return cb( err );
	  let gotit = false;
	  let msg = null;
	  async.until( 
            () => { return gotit; },
	    (cb) => {
	      this.cq.receiveQueueMessage( queue, { isPeekLock: true }, (err, _msg) => {
		if ( err && err == 'No messages to receive' ) {
		  return setTimeout( () => {
		    cb();
		  }, this.options.waitTimeSeconds * 1000 );
		}
		if ( err ) return cb( new Error( err ) );
		gotit = true;
		msg = _msg;
		cb();
	      });
	    },
	    (err) => {
	      if ( err ) return cb( err );
	      cb([{
		handle: msg,
		msg: JSON.parse( msg.body ),
	      }]);
	    });
	});
      }, cb );
    }

    _remove( queue, handle, cb ) {
      this._try( (cb) => {
	this.cq.deleteMessage( handle, cb );
      }, cb );
    }

  }

  return new AzureQ();
}
