'use strict';

let async = require( 'async' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class RabbitMQ extends CloudQueue {

    constructor() {
      super();

      let defaults = {
	visibilityTimeout: 30,
        waitTimeSeconds: 5,
        maxNumberOfMessages: 1,
      };
      this.options = Object.assign( {}, defaults, config.options );
      this.assertedQueues = {};
    }

    _producer_connect( cb ) {
      this.pch = null;
      require( 'amqplib/callback_api' ).connect( config.connection.url, ( err, conn ) => {
	if ( err ) throw( err );
	process.once('SIGINT', conn.close.bind(conn)); // close it if ctrlc

	conn.on( 'error', (err) => {
	  this.log.warn( 'RabbitMQ connection interrupted:', err );
	});

	conn.createConfirmChannel( (err, ch) => {
	  if ( err ) throw( err );
	  this.pch = ch;
	  ch.on( 'error', ( err ) => {
            this.log.error( 'RabbitMQ channel error:', err.message );
            this.pch = null;
          });
          ch.on( 'close', () => {
            this.log.warn( 'RabbitMQ channel closed, exiting.' );
            process.exit(1);
          });
          ch.on( 'blocked', ( reason ) => {
            this.log.warn( 'RabbitMQ channel blocked because:', reason );
          });

          ch.on( 'unblocked', () => {
            this.log.warn( 'RabbitMQ channel unblocked.' );
          });

	  cb();
	});
      });
    }

    _consumer_connect( queue, messageHandler ) {
      this.cch = null;
      require( 'amqplib/callback_api' ).connect( config.connection.url, ( err, conn ) => {
	if ( err ) throw( err );
	process.once('SIGINT', conn.close.bind(conn)); // close it if ctrlc

	conn.on( 'error', (err) => {
	  this.log.warn( 'RabbitMQ connection interrupted:', err );
	});

	conn.createChannel( (err, ch) => {
	  if ( err ) throw( err );
	  this.cch = ch;
	  ch.on( 'error', ( err ) => {
            this.log.error( 'RabbitMQ channel error:', err.message );
            this.cch = null;
          });
          ch.on( 'close', () => {
            this.log.warn( 'RabbitMQ channel closed, exiting.' );
            process.exit(1);
          });
          ch.on( 'blocked', ( reason ) => {
            this.log.warn( 'RabbitMQ channel blocked because:', reason );
          });

          ch.on( 'unblocked', () => {
            this.log.warn( 'RabbitMQ channel unblocked.' );
          });

	  this.cch.assertQueue( queue, { durable: true }, (err) => {
	    if ( err ) throw( err );
	    this.cch.consume( queue, (message) => {
	      let msg = JSON.parse( message.content.toString( 'utf-8' ) );
              messageHandler( msg, (err) => {
		if ( err ) return;
		this._remove( queue, message, (err) => {
		  if ( err ) this.log.error( err );
		});
              });
	    }, {noAck: false }, (err) => {
	      if ( err ) this.log.error( err );
	    });
	  });
	});
      });
    }

    _assertQueue( q, queue, cb ) {
      if ( this.assertedQueues[ queue ] ) return process.nextTick( cb );
      q.assertQueue( queue, { durable: true }, ( err ) => {
	if ( err ) return cb( err );
	this.assertedQueues[ queue ] = true;
	cb();
      });
    }

    _enqueue( queue, message, cb ) {
      this._assertQueue( this.pch, queue, ( err ) => {
	if ( err ) return cb( err );
	this.pch.sendToQueue( queue, new Buffer( JSON.stringify( message ) ), { persistent: true }, (err) => {
	  if ( err ) return cb( err );
	  this.pch.waitForConfirms( (err) => {
	    cb( err );
	  });
	});
      });
    }

    _remove( queue, handle, cb ) {
      this._try( (cb) => {
	try {
	  this.cch.ack( handle );
	  cb();
	} catch( err ) {
	  cb( err );
	}
      }, cb );
    }

  }

  return new RabbitMQ();
}
