var async = require( 'async' );
var config = require( './config' );
var log = require( 'winston' );

function exit( err ) {
  if ( err ) console.trace( err );
  process.exit( err ? 1 : 0 );
}

var qType = process.argv[2];
var qConfig = config[ qType ];
if ( ! qConfig ) {
  console.log( 'Usage: $0 qType [count]: No config found for', qType );
  process.exit(1);
}

qConfig.logger = log;
var q = require( './index' )( qConfig );

q.consumer.connect( function( err ) {
  if ( err ) exit( err );
  async.forever( function( cb ) {
    q.dequeue( 'peebtest', function( err, messages ) {
      if ( err ) return cb( err );
      if ( ! ( messages && messages[0] ) ) {
	console.log( 'no messages available' );
	return cb();
      }
      else {
	console.log( 'dequeued', messages.length, 'messages' );
      }
      async.eachSeries( messages, function( message, cb ) {
	log.info( JSON.stringify( message.msg ) );
	// do some work ,,,
	q.remove( 'peebtest', message.handle, function( err ) {
          cb( err );
	});
      }, cb );
    });
  }, function( err ) {
    log.error( 'ERROR:', err.message );
    process.exit(0);
  });
});
