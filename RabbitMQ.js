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
      });
    }

    _enqueue( queue, message, cb ) {
      this.pch.assertQueue( queue, { durable: true }, ( err ) => {
	if ( err ) return cb( err );
	this.pch.sendToQueue( queue, new Buffer( JSON.stringify( message ) ), { persistent: true }, (err) => {
	  if ( err ) return cb( err );
	  this.pch.waitForConfirms( (err) => {
	    cb( err );
	  });
	});
      });
    }

    _dequeue( queue, cb ) {
      this._try( (cb) => {
	this.cch.assertQueue( queue, { durable: true }, (err) => {
	  if ( err ) return cb( err );
	  let msg = false;
	  async.until(
	    () => { return msg !== false; },
	    (cb) => {
	      this.cch.get( queue, { noAck: false }, ( err, _msg ) => {
		if ( err ) return cb( err );
		msg = _msg;
		if ( msg == false ) return setTimeout( () => { return cb(); }, this.options.waitTimeSeconds * 1000 );
		else return cb();
	      });
	    },
	    (err) => {
	      if ( err ) return cb( err );
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
	  cb();
	} catch( err ) {
	  cb( err );
	}
      }, cb );
    }

  }

  return new RabbitMQ();
}
