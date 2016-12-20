'use strict';

let async = require( 'async' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class RabbitMQ extends CloudQueue {

    constructor() {
      super();

      let defaults = {
	producerConfirm: true,
      };
      this.options = Object.assign( {}, defaults, config.options );
      this.assertedQueues = {};
    }

    _producer_connect( cb ) {
      this.pch = null;
      require( 'amqplib/callback_api' ).connect( config.connection.url, ( err, conn ) => {
	if ( err ) return cb( err );
	process.once('SIGINT', conn.close.bind(conn)); // close it if ctrlc

	conn.on( 'error', (err) => {
	  this.log.warn( 'RabbitMQ connection interrupted:', err );
	});

	if ( this.options.producerConfirm ) {
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
	}
	else {
	  conn.createChannel( (err, ch) => {
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
	}
      });
    }

    _consumer_connect( queue, messageHandler, rcb ) {
      this.cch = null;
      require( 'amqplib/callback_api' ).connect( config.connection.url, ( err, conn ) => {
	if ( err && rcb ) return rcb( err );
	if ( err && !messageHandler ) return queue( err );
	if ( rcb ) rcb();
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

	  // set prefetch ( QoS ) http://www.squaremobius.net/amqp.node/channel_api.html#channel_prefetch
	  if ( config.options && config.options.qos )
	    this.cch.prefetch( config.options.qos.count, config.options.qos.global );

	  // dequeue mode signature
	  if ( ! messageHandler ) return queue();
	  
	  this._assertQueue( this.cch, queue, (err) => {
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
      let opts = {
	durable: true
      };
      [ 'messageTtl', 'expires' ].forEach( (param) => {
	if ( config.options && ( config.options[ param ] != undefined ) )
	  opts[ param ] = config.options[ param ];
      });
      console.log( 'using:', opts );
      q.assertQueue( queue, opts, ( err ) => {
	if ( err ) return cb( err );
	this.assertedQueues[ queue ] = true;
	cb();
      });
    }

    _enqueue( queue, message, cb ) {
      this._assertQueue( this.pch, queue, ( err ) => {
	if ( err ) return cb( err );
	let opts = {
	  persistent: true,
	};
	if ( this.options.producerConfirm ) {
	  this.pch.sendToQueue( queue, new Buffer( JSON.stringify( message ) ), opts, cb );
	}
	else {
	  let sent = this.pch.sendToQueue( queue, new Buffer( JSON.stringify( message ) ), opts );
	  if ( sent === true ) return process.nextTick( cb );
	  // honor backpressure
	  this.pch.once( 'drain', cb );
	}
      });
    }

    _dequeue( queue, cb ) {
      this._try( (cb) => {
	this._assertQueue( this.cch, queue, ( err ) => {
	  if ( err ) return cb( err );
          this.cch.get( queue, { noAck: false }, ( err, msg ) => {
            if ( err ) return cb( err );
            if ( msg == false ) return setTimeout( () => { return cb( null, [] ); }, this.options.waitTimeSeconds * 1000 );
            cb( null, [{
              handle: msg,
              msg: JSON.parse( msg.content.toString( 'utf-8' ) )
            }]);
          });
	});
      }, cb );
    }

    _remove( queue, handle, cb ) {
      this._try( (cb) => {
	try {
          this.cch.ack( handle );
          process.nextTick( cb );
	} catch( err ) {
	  process.nextTick( function() {
            cb( err );
	  });
	}
      }, cb );
    }

    _consumer_length( queue, cb ) {
      this.cch.checkQueue( queue, (err,data) => {
	if ( err ) return cb( err );
	if ( data && data.messageCount )
	  return cb( null, data.messageCount );
	return cb( null, 0 );
      });
    }

    _consumer_deleteQueue( queue, cb ) {
      this.cch.checkQueue( queue, ( err, ok ) => {
	if ( err ) return cb( err );
	if ( ! (ok && ok.queue ) ) return cb();
	this.cch.deleteQueue( queue, {}, (err) => {
	  if ( err ) return cb( err );
	  delete this.assertedQueues[ queue ];
	  cb();
	});
      });
    }

  }

  return new RabbitMQ();
}
