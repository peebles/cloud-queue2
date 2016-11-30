var async = require( 'async' );
var config = require( './config' );
var log = require( 'winston' );

var qType = process.argv[2];
var qConfig = config[ qType ];
if ( ! qConfig ) {
  console.log( 'Usage: $0 qType [count]: No config found for', qType );
  process.exit(1);
}

qConfig.logger = log;
var q = require( './index' )( qConfig );

q.consumer.connect( 'peebtest', function( msg, cb ) {
  log.info( JSON.stringify( msg ) );
  cb();
});
