'use strict';
let async = require( 'async' );
module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class IronMQ extends CloudQueue {

    constructor() {
      super();

      let defaults = {
	readDelay: 1,
        visibilityTimeout: 30,
        waitTimeSeconds: 5,
        maxNumberOfMessages: 1,
        attributes: [ 'All' ],
      };

      this.options = Object.assign( {}, defaults, config.options );
    }

    _shouldStopTrying( err ) {
      return err.message.match( /Reservation has timed out/ );
    }

    _producer_connect( cb ) {
      let iron_mq = require( 'iron_mq' );
      this.pq = new iron_mq.Client( config.connection );
      process.nextTick( cb );
    }

    _consumer_connect( queue, messageHandler, rcb ) {
      let iron_mq = require( 'iron_mq' );
      this.cq = new iron_mq.Client( config.connection );

      // dequeue mode signature
      if ( rcb ) rcb();
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

    }

    _enqueue( queue, message, cb ) {
      let q = this.pq.queue( queue );
      q.post( JSON.stringify( message ), cb );
    }

    _dequeue( queue, cb ) {
      this._try( (cb) => {
	let q = this.cq.queue( queue );
	let opts = {
          n: this.options.maxNumberOfMessages,
          timeout: this.options.visibilityTimeout,
          wait: this.options.waitTimeSeconds,
	};
	q.reserve( opts, function( err, _message ) {
	  if ( err ) return cb( err );
          if ( ! _message ) return cb( null, [] );
          var messages = _message;
          if ( opts.n == 1 ) messages = [ _message ];
          cb( null, messages.map( ( message ) => {
            return {
              handle: message,
              msg: JSON.parse( message.body ),
            };
          }));
	});
      }, cb );
    }

    _remove( queue, handle, cb ) {
      this._try( (cb) => {
	let q = this.cq.queue( queue );
	let opts = {};
	if ( handle.reservation_id ) opts.reservation_id = handle.reservation_id;
	q.del( handle.id, opts, cb );
      }, cb );
    }

    _consumer_length( queue, cb ) {
      this._try( (cb) => {
	let q = this.cq.queue( queue );
	q.info( (err,body) => {
	  if ( err ) return cb( null, 0 ); // At least right now, this is what ironmq returns if a q does not exist
	  if ( ! ( body && body.size ) ) return cb( null, 0 );
	  return cb( null, body.size );
	});
      }, cb );
    }

    _consumer_deleteQueue( queue, cb ) {
      this._try( (cb) => {
	let q = this.cq.queue( queue );
	q.del_queue( cb );
      }, cb );
    }

  }

  return new IronMQ();
}
