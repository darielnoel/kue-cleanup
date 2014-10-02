var kue = require('kue'),
    util = require('util'),
    noop = function() {},
    jobs,
    CLEANUP_MAX_FAILED_TIME = 30 * 24 * 60 * 60 * 1000, // 30 days
    CLEANUP_MAX_ACTIVE_TIME = 0 * 24 * 60 * 60 * 1000, // 0 day
    CLEANUP_MAX_COMPLETE_TIME = 5 * 24 * 60 * 60 * 1000, // 5 days
    CLEANUP_INTERVAL = 5 * 60 * 1000, // 5 minutes

    KueCleanup = {
        /**
         * Initialization method
         * @author Dariel Noel <darielnoel@gmail.com>
         * @since  2014-09-29
         * @param  {object}   config Config object
         * @return {undefined}
         */
        init: function(config) {
            var instance = KueCleanup;

            instance.config = config || {};

            jobs = kue.createQueue();

            instance.setupJobs(instance.config);
        },

        /**
         * Basic configurations
         * @author Dariel Noel <darielnoel@gmail.com>
         * @since  2014-09-29
         * @param  {object}   config Basic config
         * @return {undefined}
         */
        setupJobs: function (config) {
            jobs.maxFailedTime = config.maxFailedTime ||
                CLEANUP_MAX_FAILED_TIME;
            jobs.cleanupMaxActiveTime = config.cleanupMaxActiveTime ||
                CLEANUP_MAX_ACTIVE_TIME;
            jobs.cleanupMaxCompleteTime = config.cleanupMaxCompleteTime ||
                CLEANUP_MAX_COMPLETE_TIME;
            jobs.cleanupInterval = config.cleanupInterval ||
                CLEANUP_INTERVAL;
        },

        /**
         * Clear all Kue jobs
         * @author Dariel Noel <darielnoel@gmail.com>
         * @since  2014-09-29
         * @param  {Function} callback Function to be executed after all jobs
         *                             are removed
         */
        cleanupAll: function(callback){
            performCleanup(callback);
        },

        /**
         * Clear al jobs periodically
         * @author Dariel Noel <darielnoel@gmail.com>
         * @since  2014-09-29
         * @param  {[type]}   cleanupInterval The interval amount
         */
        periodicCleanup: function(cleanupInterval){
            var instance = KueCleanup,
                currentCleanupInterval = cleanupInterval ||
                    jobs.cleanupInterval;
            instance.stopCleanup();
            intervalHandle = setInterval(performCleanup, currentCleanupInterval);
        },

        /**
         * Stop de periodically cleanup
         * @author Dariel Noel <darielnoel@gmail.com>
         * @since  2014-09-29
         */
        stopCleanup: function(){
            var instance = KueCleanup;
            clearInterval(instance.intervalHandle);
        },

        config: {},

        intervalHandle: {}

    };


module.exports = {
    init: KueCleanup.init,
    cleanupAll: KueCleanup.cleanupAll,
    periodicCleanup: KueCleanup.periodicCleanup,
    stopCleanup: KueCleanup.stopCleanup
};

/**
 * Simple log action
 * @author Dariel Noel <darielnoel@gmail.com>
 * @since  2014-09-29
 * @param  {string}   message Message to print
 */
function queueActionLog (message) {
  this.message = message || 'queueActionLog :: got an action for job id(%s)';

  this.apply = function(job) {
    console.log(util.format(this.message, job.id));
    return true;
  };
}

/**
 * Simple log action
 * @author Dariel Noel <darielnoel@gmail.com>
 * @since  2014-09-29
 * @param  {int}   age
 */
function queueActionRemove (age) {
  this.age = age;

  this.apply = function(job) {
    job.remove(noop);
    return true;
  };
}

/**
 * Filter by age
 * @author Dariel Noel <darielnoel@gmail.com>
 * @since  2014-09-29
 * @param  {[type]}   age Milleseconds
 */
function queueFilterAge (age) {
  this.now = new Date();
  this.age = age;

  this.test = function(job) {
    var created = new Date(parseInt(job.created_at));
    var age = parseInt(this.now - created);

    return age > this.age;

  };
}

/**
 * The queue iterator
 * @author Dariel Noel <darielnoel@gmail.com>
 * @since  2014-09-29
 * @param  {[type]}   ids              Jobs ids
 * @param  {[type]}   queueFilterChain Filter Chain
 * @param  {[type]}   queueActionChain Action Chain
 * @param  {Function} callback         callback
 */
function queueIterator (ids, queueFilterChain, queueActionChain, callback) {
    var count = ids.length;
    ids.forEach(function(id, index) {
        // get the kue job
        kue.Job.get(id, function(err, job) {
            if (err || !job) return;
          var filterIterator = function(filter) { return filter.test(job); };
          var actionIterator = function(filter) { return filter.apply(job); };

          // apply filter chain
          if(queueFilterChain.every(filterIterator)) {

            // apply action chain
            queueActionChain.every(actionIterator);
          }
          count--;
          if(count === 0){
            callback();
          }
        });
    });
    if(count===0){
        callback();
    }
}

/**
 * Clear all
 * @author Dariel Noel <darielnoel@gmail.com>
 * @since  2014-09-29
 * @param  {Function} callback callback
 */
function performCleanup (callback) {
  var ki = new kue();
  KueCleanup.ki = ki;
  clearState('failed', ki, function(){
    clearState('active', ki, function(){
      clearState('complete', ki, function(){
        clearState('inactive', ki, function(){
          callback();
        });
      });
    });
  });
}

/**
 * ClearState
 * @author Dariel Noel <darielnoel@gmail.com>
 * @since  2014-09-29
 * @param  {[type]}   state
 * @param  {[type]}   ki
 * @param  {Function} callback
*/
function clearState(state, ki, callback){
  ki[state](function(err, ids) {
    if (!ids || ids.length === 0){
      callback();
    } else{
      queueIterator(
        ids,
        [new queueFilterAge(jobs.cleanupMaxActiveTime)],
        [
            new queueActionLog('Going to remove job id(%s) for being active too long'),
            new queueActionRemove()
        ],
        function(){
            callback();
        }
      );
    }
  });
}

/**
 * Helper function
 * @author Dariel Noel <darielnoel@gmail.com>
 * @since  2014-09-29
 * @return {[type]}   [description]
 */
function noop(){}
