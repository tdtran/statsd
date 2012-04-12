versionstring = "statsd-librato/1.3"

var dgram      = require('dgram')
  , sys        = require('util')
  , net        = require('net')
  , config     = require('./config')
  , _          = require('underscore')
  , async      = require('async')
  , url_parse  = require('url').parse
  , https      = require('https')
  , http       = require('http')
  , syslog     = require('node-syslog');

var logger = function(severity,message){
  if(severity < syslog.LOG_NOTICE){
    console.error(message);
  } else {
    console.log(message);
  }
}

var counters = {};
var timers = {};
var gauges = {};
var debugInt, flushInt, server, mgmtServer;
var startup_time = Math.round(new Date().getTime() / 1000);

var globalstats = {
  graphite: {
    last_flush: startup_time,
    last_exception: startup_time
  },
  messages: {
    last_msg_seen: startup_time,
    bad_lines_seen: 0,
  }
};

process.on('uncaughtException', function (err) {
  logger(syslog.LOG_ERR, 'Caught exception: ' + err);
});

config.configFile(process.argv[2], function (config, oldConfig) {
  if (config.debug){
    sys.log('logging to stdout/err');
  } else {
    syslog.init("node-syslog", syslog.LOG_PID | syslog.LOG_ODELAY, syslog.LOG_LOCAL0);
    sys.log('logging to syslog');
    logger = syslog.log;
  }

  function graphServiceIs(name){
    return (config.graphService) && (config.graphService == name);
  }

  if (!config.debug && debugInt) {
    clearInterval(debugInt); 
    debugInt = false;
  }

  if (config.debug) {
    if (debugInt !== undefined) { clearInterval(debugInt); }
    debugInt = setInterval(function () { 
      logger(syslog.LOG_INFO, "Counters:\n" + sys.inspect(counters) + "\nGauges:\n" + sys.inspect(gauges) + "\nTimers:\n" + sys.inspect(timers));
    }, config.debugInterval || 10000);
  }

  if (server === undefined) {
    server = dgram.createSocket('udp4', function (msg, rinfo) {
      var msgStr = msg.toString().replace(/^\s+|\s+$/g,"").replace(/\u0000/g, '');
      if (msgStr.length == 0) {
        if (config.debug) {
          logger(syslog.LOG_DEBUG, 'No messsages.');
        }
        return;
      }
      if (config.dumpMessages) {
        logger(syslog.LOG_INFO, 'Messages: ' + msgStr);
      }
      var bits = msgStr.split(':');
      var key = '';
      key = bits.shift();

      if (bits.length == 0) {
        return;
      }

      for (var i = 0; i < bits.length; i++) {
        var sampleRate = 1;
        var fields = bits[i].split("|");
        if (fields[1] === undefined) {
            logger(syslog.LOG_ERR, 'Bad line: ' + fields);
            globalstats['messages']['bad_lines_seen']++;
            continue;
        }
        if (fields[1].trim() == "ms") {
          if (! timers[key]) {
            timers[key] = [];
          }
          timers[key].push(Number(fields[0] || 0));
        } else if (fields[1].trim() === "g") {
          gauges[key] = Number(fields[0] || 0);
        } else {
          if (fields[2] && fields[2].match(/^@([\d\.]+)/)) {
            sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
          }
          if (! counters[key]) {
            counters[key] = 0;
          }
          counters[key] += Number(fields[0] || 1) * (1 / sampleRate);
        }
      }

      globalstats['messages']['last_msg_seen'] = Math.round(new Date().getTime() / 1000);
    });

    server.on("listening", function () {
      var address = server.address();
      logger(syslog.LOG_INFO, "statsd is running on " + address.address + ":" + address.port);
      sys.log("server is up");
    });

    mgmtServer = net.createServer(function(stream) {
      stream.setEncoding('ascii');

      stream.on('data', function(data) {
        var cmd = data.trim();

        switch(cmd) {
          case "help":
            stream.write("Commands: stats, counters, gauges, timers, quit\n\n");
            break;

          case "stats":
            var now    = Math.round(new Date().getTime() / 1000);
            var uptime = now - startup_time;

            stream.write("uptime: " + uptime + "\n");

            for (group in stats) {
              for (metric in stats[group]) {
                var val;

                if (metric.match("^last_")) {
                  val = now - stats[group][metric];
                }
                else {
                  val = stats[group][metric];
                }

                stream.write(group + "." + metric + ": " + val + "\n");
              }
            }
            stream.write("END\n\n");
            break;

          case "counters":
            stream.write(sys.inspect(counters) + "\n");
            stream.write("END\n\n");
            break;

          case "timers":
            stream.write(sys.inspect(timers) + "\n");
            stream.write("END\n\n");
            break;

          case "gauges":
            stream.write(sys.inspect(gauges) + "\n");
            stream.write("END\n\n");
            break;
          
          case "quit":
            stream.end();
            break;

          default:
            stream.write("ERROR\n");
            break;
        }

      });
    });

    server.bind(config.port || 8125);
    mgmtServer.listen(config.mgmt_port || 8126);

    var flushInterval = Number(config.flushInterval || 10000);

    flushInt = setInterval(function () {
      var stats = {};
      stats["gauges"] = {};
      stats["counters"] = {};
      var ts = Math.round(new Date().getTime() / 1000);
      var numStats = 0;
      var key;

      for (key in counters) {
        var stat;
        stat = stats["counters"];

        var value = counters[key];
        stat[key] = {};
        stat[key]["value"] = value;

        counters[key] = 0;

        numStats += 1;
      }

      for (key in gauges) {
        var stat;
        stat = stats["gauges"];

        var value = gauges[key];
        stat[key] = {};
        stat[key]["value"] = value;

        gauges[key] = 0;

        numStats += 1;
      }
      
      for (key in timers) {
        if (timers[key].length > 0) {
          var pctThreshold = config.percentThreshold || 90;
          var values = timers[key].sort(function (a,b) { return a-b; });
          var count = values.length;
          var min = values[0];
          var max = values[count - 1];

          var mean = min;
          var maxAtThreshold = max;

          if (count > 1) {
            var thresholdIndex = Math.round(((100 - pctThreshold) / 100) * count);
            var numInThreshold = count - thresholdIndex;
            values_sliced = values.slice(0, numInThreshold);
            maxAtThreshold = values_sliced[numInThreshold - 1];
            // average the remaining timings
            var sum = 0;
            for (var i = 0; i < numInThreshold; i++) {
              sum += values_sliced[i];
            }
            mean = sum / numInThreshold;
          }

          var sum = 0;
          var sumOfSquares = 0;
          for (var i = 0; i < count; i++) {
            sum += values[i];
            sumOfSquares += values[i] * values[i];
          }

          timers[key] = [];
          stats["gauges"][key] = {};
          stats["gauges"][key]["count"] = count;
          stats["gauges"][key]["sum"] = sum;
          stats["gauges"][key]["sum_squares"] = sumOfSquares;
          stats["gauges"][key]["min"] = min;
          stats["gauges"][key]["max"] = max;
          if (!graphServiceIs("librato-metrics")){
            stats["gauges"][key]["upper_" + pctThreshold] = maxAtThreshold;
            stats["gauges"][key]["mean"] = mean;
          }

          numStats += 1;
        }
      }

      stats["counters"]["numStats"] = {};
      stats["counters"]["numStats"]["value"] = numStats;

      var slicey = function(obj,slicelen){
        var slicecounter = 0;
        var groups = _.groupBy(obj,function (num){ var ret = Math.floor(slicecounter/slicelen); slicecounter += + 1; return ret;});
        return _.map(groups,function(k,v){ return k; });
      }

      function build_hash(type){
        return function(group){
          if (graphServiceIs("librato-metrics")){
            stat = stats["gauges"];
          }
          var hash = {};
          hash[type] = {};
          _.each(group,function(metric){
            hash[type][metric] = stats[type][metric];
          });
          if (graphServiceIs("librato-metrics")){
            hash["measure_time"] = ts
          }
          return hash;
        };
      }

      function hash_postprocess(inhash){
        if (graphServiceIs("librato-metrics")){
          var hash = {};
          hash["gauges"] = inhash["gauges"] || {};
          if (_.include(_.keys(inhash),"counters")) {
            _.each(_.keys(inhash["counters"]),function(metric){
                hash["gauges"][metric] = inhash["counters"][metric];
                });
          }
          snap = config.libratoSnap || 10;
          hash["measure_time"] = (ts - (ts%snap));
          if (config.libratoSource) { hash["source"] = config.libratoSource; }
          return hash;
        } else {
          return inhash;
        }
      }

      function build_string(hash,type){
          var stats_str ='';

          if (graphServiceIs("librato-metrics")){
            stats_str = JSON.stringify(hash);
          } else {
            for (key in hash[type]){
              if (graphServiceIs("librato-metrics")){
                k =       key.replace(/[^-.:_\w]+/, '_').substr(0,255)
              } else {
                k =       key
                          .replace(/\s+/g, '_')
                          .replace(/\//g, '-')
                          .replace(/[^a-zA-Z_\-0-9\.]/g, '');
              }

              var stat = hash[type][k];
              if (type == "counters"){
                if(k == 'numStats'){
                  stats_str += 'statsd.numStats ' + stat['value'] + ' ' + ts + "\n";
                } else if (stat["value"] > 0){
                  stats_str += ('stats.counters.' + k + ' ' + stat["value"] + ' ' + ts + "\n");
                }
              } else if (type == "gauges") {
                if (stat["value"] > 0){
                  stats_str += ('stats.gauges.' + k + ' ' + stat['value'] + ' ' + ts + "\n");
                }
              } else {
                for (s in stat){
                  stats_str += ('stats.timers.' + k + '.' + s + ' ' + stat[s] + ' ' + ts + "\n");
                }
              }
            }
          }
          return stats_str;
      };

      var slice_length = config.batch || 200;
      var ggroups = slicey(_.keys(stats["gauges"]),slice_length);
      var cgroups = slicey(_.keys(stats["counters"]),slice_length);
      var ghashes = _.map(ggroups,build_hash("gauges"));
      var chashes = _.map(cgroups,build_hash("counters"));
      var combined_hashes = ghashes.concat(chashes);

      var logerror = function(e){
        if (e){
          globalstats['graphite']['last_exception'] = Math.round(new Date().getTime() / 1000);
          if(config.debug) {
            logger(syslog.LOG_DEBUG, e);
          }
        }
      }

      var submit_to_librato = function(stats_str,retry){
        var parsed_host = url_parse(config.libratoHost || 'https://metrics-api.librato.com');
        var options = {
          host: parsed_host["hostname"],
          port: parsed_host["port"] || 443,
          path: '/v1/metrics.json',
          method: 'POST',
          headers: {
            "Authorization": 'Basic ' + new Buffer(config.libratoUser + ':' + config.libratoApiKey).toString('base64'),
            "Content-Length": stats_str.length,
            "Content-Type": "application/json",
            "User-Agent" : versionstring
          }
        };

        var proto = http;
        if ((parsed_host["protocol"] || 'http:').match(/https/)){
          proto = https;
        }
        var req = proto.request(options, function(res) {
          if(res.statusCode != 204){
            res.on('data', function(d){
              var errdata = "HTTP " + res.statusCode + ": " + d;
              if (retry){
                if (config.debug) { console.log("received error " + res.statusCode + " connecting to Librato, retrying... "); }
                setTimeout(function(){
                  submit_to_librato(stats_str,false);
                }, Math.floor(flushInterval/2) + 100);
              } else {
                logger(syslog.LOG_CRIT, "Error connecting to Librato!\n" + errdata);
              }
            });
          }
        });
        req.write(stats_str);
        req.end();
        globalstats['graphite']['last_flush'] = Math.round(new Date().getTime() / 1000);
        req.on('error', function(errdata) {
            if (retry){
              setTimeout(function(){
                submit_to_librato(stats_str,false);
              }, Math.floor(flushInterval/2) + 100);
            } else {
              lokger(syslog.LOG_CRIT, "Error connecting to Librato!\n" + errdata);
            }
        });
      }

      var concurrent_conns = config.maxConnections || 10;
      var submissionq = async.queue(function (task,cb){
          task();
          cb();
          },concurrent_conns);

      _.each(combined_hashes,function(hash){
        _.each(_.keys(hash), function(hashtype) { /* 'gauges' and 'counters' */
          if (graphServiceIs("librato-metrics")){
            var stats_str = build_string(hash_postprocess(hash),hashtype)
            submissionq.push(function(){
              if (config.debug) {
                logger(syslog.LOG_DEBUG, stats_str);
                logger(syslog.LOG_DEBUG, stats_str.length);
              }
              submit_to_librato(stats_str,true);
            }, logerror);
          } else {
            var stats_str = build_string(hash_postprocess(hash),hashtype)
            submissionq.push(function(){
              if (config.debug) {
                logger(syslog.LOG_DEBUG, stats_str);
                logger(syslog.LOG_DEBUG, stats_str.length);
              }

              try {
                var graphite = net.createConnection(config.graphitePort, config.graphiteHost);
                graphite.addListener('error', function(connectionException){
                  if (config.debug) {
                    logger(syslog.LOG_CRIT, connectionException);
                  }
                });
                graphite.on('connect', function() {
                  this.write(stats_str);
                  this.end();
                  globalstats['graphite']['last_flush'] = Math.round(new Date().getTime() / 1000);
                  });
              } catch(e){
                if (config.debug) {
                  logger(syslog.LOG_DEBUG, e);
                }
              }
            }, logerror);
          }
        });
      });
    }, flushInterval);
  }
});
