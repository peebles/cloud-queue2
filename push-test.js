var async = require( 'async' );
var config = require( './config' );
var log = require( 'winston' );

function exit( err ) {
  if ( err ) console.trace( err );
  process.exit( err ? 1 : 0 );
}

var qType = process.argv[2];
var count = process.argv[3] || 100;
var qConfig = config[ qType ];
if ( ! qConfig ) {
  console.log( 'Usage: $0 qType [count]: No config found for', qType );
  process.exit(1);
}

qConfig.logger = log;
var q = require( './index' )( qConfig );

var iter  = 1;
var msg = {
  payload: 'This is a Test',
};

var deviceIds = [ '1111', '2222', '3333', '4444' ];
function deviceId() {
  return deviceIds[ Math.floor(Math.random() * deviceIds.length) ];
}

q.producer.connect( function( err ) {
  if ( err ) exit( err );
  
  async.whilst( 
    function() { return ( iter <= count ); },
    function( cb ) {
      msg.seq = iter;
      msg.deviceId = deviceId();
      console.log( 'enqueue:', JSON.stringify( msg ) );
      q.producer.send( 'peebtest', msg, function( err ) {
	if ( err ) return cb( err );
	iter += 1;
	cb();
      });
    },
    function( err ) {
      if ( err ) log.error( 'ERROR:', err.message );
      console.log( 'done enqueuing messages, waiting forever ...' );
      async.forever(
	function( cb ) {
	  setTimeout( function() { cb(); }, 1000 );
	},
	function(err) {
	  console.log( err );
	  process.exit(1);
	});
    }
  );
});

