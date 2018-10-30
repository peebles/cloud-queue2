'use strict';

let async = require( 'async' );
let shortid = require( 'shortid' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class RedisQ extends CloudQueue {

    constructor() {
      super();

      let defaults = {
        waitTimeSeconds: 5,
      };
      this.options = Object.assign( {}, defaults, config.options );
      this.pReadyCalled = false;
      this.cReadyCalled = false;
    }

    // Over ride the super class try() method, since Redis already takes care of retrying
    _try( fcn, cb ) {
      return fcn( cb );
    }

    _producer_connect( cb ) {
      this.pq = require( 'redis' ).createClient( config.connection );
      this.pq.on( 'error', (err) => {
	// this prevents process from exiting and redis
        // will try to reconnect...
	this.log.warn( err.message );
      });
      this.pq.on( 'reconnecting', (o) => {
        this.log.warn( `redis reconnecting: attempt: ${o.attempt}, delay: ${o.delay}` );
      });
      this.pq.on( 'ready', () => {
	if ( this.pReadyCalled ) {
	  this.log.warn( 'producer on-ready called again!' );
	  return;
	}
	this.pReadyCalled = true;
	cb();
      });
    }

    _consumer_connect( queue, messageHandler, rcb ) {
      this.cq = require( 'redis' ).createClient( config.connection );
      if ( rcb ) rcb();
      this.cq.on( 'error', (err) => {
	// this prevents process from exiting and redis
        // will try to reconnect...
	this.log.warn( err.message );        
      });
      this.cq.on( 'reconnecting', (o) => {
        this.log.warn( `redis reconnecting: attempt: ${o.attempt}, delay: ${o.delay}` );
      });
      this.cq.on( 'ready', () => {

	if ( this.cReadyCalled ) {
	  this.log.warn( 'consumer on-ready called again!' );
	  return;
	}
	this.cReadyCalled = true;

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

    _enqueue( queue, message, cb ) {
      let uuid = shortid.generate(); // create a key to store the message
      this.pq.set( uuid, JSON.stringify( message ), (err) => {
	if ( err ) return cb( err );
	// now push the uuid on the queue
        this.pq.lpush( queue, uuid, cb );
      });
    }

    _dequeue( queue, cb ) {
      this.cq.rpop( queue, ( err, uuid ) => {
        if ( err ) return cb( err );
        if ( ! uuid ) {
          setTimeout( () => { cb( null, [] ); }, this.options.waitTimeSeconds * 1000 );
        }
        else {
          this.cq.get( uuid, ( err, msg ) => {
            if ( err ) return cb( err );
            if ( ! msg ) return cb( new Error( 'redis: could not find uuid: ' + uuid ) );
            this.cq.del( uuid, ( err ) => {
              if ( err ) return cb( err );
              cb( null, [{
		handle: null,
		msg: JSON.parse( msg )
              }]);
	    });
	  });
	}
      });
    }

    _remove( queue, handle, cb ) {
      // there is no remove in redis
      process.nextTick( cb );
    }

    _consumer_length( queue, cb ) {
      this.cq.llen( queue, cb );
    }
    
  }

  return new RedisQ();
}
