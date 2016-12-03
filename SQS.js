'use strict';
let async = require( 'async' );

module.exports = function( config ) {

  let CloudQueue = require( './CloudQueue' )( config );

  class SQS extends CloudQueue {

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

      let AWS = require( 'aws-sdk' );
      AWS.config.update( config.connection );
      this.q = new AWS.SQS();
    }

    _producer_connect( cb ) {
      let AWS = require( 'aws-sdk' );
      AWS.config.update( config.connection );
      this.pq = new AWS.SQS();
      process.nextTick( cb );
    }

    _consumer_connect( queue, messageHandler, rcb ) {
      let AWS = require( 'aws-sdk' );
      AWS.config.update( config.connection );
      this.cq = new AWS.SQS();

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
      this.pq.createQueue({ QueueName: queue }, ( err, data ) => {
	if ( err ) return cb( err );
	let url = data.QueueUrl;
	this.pq.sendMessage({ QueueUrl: url, MessageBody: JSON.stringify( message ), DelaySeconds: 0 }, ( err ) => {
	  cb( err );
	});
      });
    }

    _dequeue( queue, cb ) {
      this._try( (cb) => {
	this.cq.createQueue({ QueueName: queue }, ( err, data ) => {
	  if ( err ) return cb( err );
	  let url = data.QueueUrl;
	  this.cq.receiveMessage({
	    QueueUrl: url,
            AttributeNames: this.options.attributes,
            MaxNumberOfMessages: this.options.maxNumberOfMessages,
            VisibilityTimeout: this.options.visibilityTimeout,
            WaitTimeSeconds: this.options.waitTimeSeconds
	  }, (err, data ) => {
	    if ( err ) return cb( err );
	    if ( ! ( data && data.Messages ) ) return cb( null, [] );
	    let msgs = [];
	    data.Messages.forEach( (message) => {
	      msgs.push({
		handle: message.ReceiptHandle,
		msg: JSON.parse( message.Body )
	      });
	    });
	    cb( null, msgs );
	  });
	});
      }, cb );
    }

    _remove( queue, handle, cb ) {
      this._try( (cb) => {
	this.cq.createQueue({ QueueName: queue }, ( err, data ) => {
	  if ( err ) return cb( err );
	  let url = data.QueueUrl;
	  this.cq.deleteMessage({ QueueUrl: url, ReceiptHandle: handle}, (err) => {
	    cb( err );
	  });
	});
      }, cb );
    }

    _consumer_length( queue, cb ) {
      this._try( (cb) => {
	this.cq.createQueue({ QueueName: queue }, ( err, data ) => {
	  if ( err ) return cb( err );
	  let url = data.QueueUrl;
	  this.cq.getQueueAttributes({ QueueUrl: url, AttributeNames: [ 'ApproximateNumberOfMessages' ] }, (err, data) => {
	    if ( err ) return cb( err );
	    if ( data && data.Attributes && data.Attributes.ApproximateNumberOfMessages )
	      return cb( null, data.Attributes.ApproximateNumberOfMessages );
	    return cb( null, 0 );
	  });
	});
      }, cb );      
    }

    _consumer_deleteQueue( queue, cb ) {
      this._try( (cb) => {
	this.cq.createQueue({ QueueName: queue }, ( err, data ) => {
	  if ( err ) return cb( err );
	  let url = data.QueueUrl;
	  this.cq.deleteQueue({ QueueUrl: url }, cb );
	});
      }, cb );      
    }

  }

  return new SQS();
}
