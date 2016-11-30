'use strict';

let async = require( 'async' );
let shortid = require( 'shortid' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class RedisQ extends CloudQueue {

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
      this.pq = require( 'redis' ).createClient( config.connection );
      this.pq.on( 'error', (err) => {
	// this prevents process from exiting and redis
        // will try to reconnect...
	this.log.warn( err );
      });
      this.pq.on( 'ready', cb );      
    }

    _consumer_connect( queue, messageHandler ) {
      this.cq = require( 'redis' ).createClient( config.connection );
      this.cq.on( 'error', (err) => {
	// this prevents process from exiting and redis
        // will try to reconnect...
	this.log.warn( err );
      });
      this.cq.on( 'ready', () => {
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

    _enqueue( queue, message, cb ) {
      let uuid = shortid.generate(); // create a key to store the message
      this.pq.set( uuid, JSON.stringify( message ), (err) => {
	if ( err ) return cb( err );
	// now push the uuid on the queue
        this.pq.lpush( queue, uuid, cb );
      });
    }

    _dequeue( queue, cb ) {
      let msg = null;
      async.until(
        () => { return msg != null; },
        ( cb ) => {
          this.cq.rpop( queue, ( err, uuid ) => {
            if ( err ) return cb( err );
            if ( ! uuid ) {
              setTimeout( () => { cb(); }, this.options.waitTimeSeconds * 1000 );
            }
            else {
              this.cq.get( uuid, ( err, _msg ) => {
                if ( err ) return cb( err );
                if ( ! _msg ) return cb( new Error( 'redis: could not find uuid: ' + uuid ) );
                this.cq.del( uuid, ( err ) => {
                  if ( err ) return cb( err );
                  msg = _msg;
                  cb();
                });
              });
            }
          });
        },
        ( err ) => {
          if ( err ) return cb( err );
          cb( null, [{
            handle: null,
            msg: JSON.parse( msg )
          }]);
        });
    }

    _remove( queue, handle, cb ) {
      // there is no remove in redis
      process.nextTick( cb );
    }

  }

  return new RedisQ();
}
