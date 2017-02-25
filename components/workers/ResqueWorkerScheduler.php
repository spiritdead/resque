<?php

namespace spiritdead\resque\components\workers;

use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\components\workers\base\ResqueWorkerBase;
use spiritdead\resque\components\workers\base\ResqueWorkerInterface;
use spiritdead\resque\plugins\ResqueScheduler;
use spiritdead\resque\Resque;

class ResqueWorkerScheduler extends ResqueWorkerBase implements ResqueWorkerInterface
{
    const LOG_NONE = 0;
    const LOG_NORMAL = 1;
    const LOG_VERBOSE = 2;

    /**
     * @var null|ResqueScheduler
     */
    protected $resqueInstance;

    /**
     * @var int Current log level of this worker.
     */
    public $logLevel = 0;

    /**
     * @var int Interval to sleep for between checking schedules.
     */
    protected $interval = 5;

    /**
     * @var boolean True if on the next iteration, the worker should shutdown.
     */
    private $shutdown = false;

    /**
     * @var boolean True if this worker is paused.
     */
    private $paused = false;

    /**
     * @var boolean for determinate if this worker is working
     */
    private $working = false;

    /**
     * @var string String identifying this worker.
     */
    private $id;

    /**
     * Instantiate a new worker, given a list of queues that it should be working
     * on. The list of queues should be supplied in the priority that they should
     * be checked for jobs (first come, first served)
     *
     * Passing a single '*' allows the worker to work on all queues in alphabetical
     * order. You can easily add new queues dynamically and have them worked on using
     * this method.
     *
     * @param string|array $queues String with a single queue name, array with multiple.
     */
    public function __construct(ResqueScheduler $resqueInst,$queues)
    {
        parent::__construct($resqueInst,$queues);
        $this->hostname = php_uname('n');

        $this->id = $this->hostname . ':' . getmypid() . ':schedule';
    }

    /**
     * @param ResqueJobBase $job
     */
    public function perform(ResqueJobBase $job)
    {
        // TODO: Implement perform() method.
    }

    /**
     * The primary loop for a worker.
     *
     * Every $interval (seconds), the scheduled queue will be checked for jobs
     * that should be pushed to Resque.
     *
     * @param int $interval How often to check schedules.
     */
    public function work($interval = self::DEFAULT_INTERVAL,$blockinge = false)
    {
        if ($interval !== null) {
            $this->interval = $interval;
        }

        $this->updateProcLine('Starting');
        $this->startup();

        while (true) {
            if ($this->shutdown) {
                break;
            }
            if (!$this->paused) {
                $this->handleDelayedItems();
            } else {
                $this->updateProcLine('Paused');
            }
            $this->updateProcLine('Waiting for new jobs');
            sleep($this->interval);
        }

        $this->unregisterWorker();
    }

    /**
     * Handle delayed items for the next scheduled timestamp.
     *
     * Searches for any items that are due to be scheduled in Resque
     * and adds them to the appropriate job queue in Resque.
     *
     * @param \DateTime|int $timestamp Search for any items up to this timestamp to schedule.
     */
    public function handleDelayedItems($timestamp = null)
    {
        while (($oldestJobTimestamp = $this->resqueInstance->nextDelayedTimestamp($timestamp)) !== false) {
            $this->updateProcLine('Processing Delayed Items');
            $this->enqueueDelayedItemsForTimestamp($oldestJobTimestamp);
        }
    }

    /**
     * Schedule all of the delayed jobs for a given timestamp.
     *
     * Searches for all items for a given timestamp, pulls them off the list of
     * delayed jobs and pushes them across to Resque.
     *
     * @param \DateTime|int $timestamp Search for any items up to this timestamp to schedule.
     */
    public function enqueueDelayedItemsForTimestamp($timestamp)
    {
        $item = null;
        while ($item = $this->resqueInstance->nextItemForTimestamp($timestamp)) {
            $this->workingOn($item);
            $this->log('queueing ' . $item['class'] . ' in ' . $item['queue'] . ' [delayed]');

            $this->resqueInstance->events->trigger('beforeDelayedEnqueue', [
                'queue' => $item['queue'],
                'class' => $item['class'],
                'args' => $item['args']
            ]);

            $payload = array_merge([$item['queue'], $item['class']], $item['args']);
            call_user_func_array('Resque::enqueue', $payload);
            $this->doneWorking();
        }
    }

    /**
     * Perform necessary actions to start a worker.
     */
    protected function startup()
    {
        $this->registerSigHandlers();
        $this->pruneDeadWorkers();
        $this->resqueInstance->events->trigger('beforeFirstFork', $this);
        $this->registerWorker();
    }

    /**
     * Look for any workers which should be running on this server and if
     * they're not, remove them from Redis.
     *
     * This is a form of garbage collection to handle cases where the
     * server may have been killed and the Resque workers did not die gracefully
     * and therefore leave state information in Redis.
     */
    public function pruneDeadWorkers()
    {
        $workerPids = self::workerPids();
        $workers = self::all($this->resqueInstance);
        /* @var self $worker */
        foreach ($workers as $worker) {
            if (is_object($worker)) {
                list($host, $pid) = explode(':', (string)$worker, 2);
                if ($host != $this->hostname || in_array($pid, $workerPids) || $pid == getmypid()) {
                    continue;
                }
                $worker->unregisterWorker();
            }
        }
    }

    /**
     * Register this worker in Redis.
     */
    public function registerWorker()
    {   
        $this->resqueInstance->redis->sadd('worker-schedulers', (string)$this);
        $this->resqueInstance->redis->set('worker-scheduler:' . (string)$this . ':started', strftime('%a %b %d %H:%M:%S %Z %Y'));
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     */
    public function unregisterWorker()
    {
        $id = (string)$this;
        $this->resqueInstance->redis->srem('worker-schedulers', $id);
        $this->resqueInstance->redis->del('worker-scheduler:' . $id);
        $this->resqueInstance->redis->del('worker-scheduler:' . $id . ':started');
        $this->resqueInstance->stats->clear('processed:' . $id);
        $this->resqueInstance->stats->clear('failed:' . $id);
    }

    /**
     * Register signal handlers that a worker should respond to.
     *
     * TERM: Shutdown immediately and stop processing jobs.
     * INT: Shutdown immediately and stop processing jobs.
     * QUIT: Shutdown after the current job finishes processing.
     * USR1: Kill the forked child immediately and continue processing jobs.
     */
    private function registerSigHandlers()
    {
        if (!function_exists('pcntl_signal')) {
            return;
        }

        pcntl_signal(SIGTERM, [$this, 'shutDownNow']);
        pcntl_signal(SIGINT, [$this, 'shutDownNow']);
        pcntl_signal(SIGQUIT, [$this, 'shutdown']);
        pcntl_signal(SIGUSR2, [$this, 'pauseProcessing']);
        pcntl_signal(SIGCONT, [$this, 'unPauseProcessing']);
    }

    /**
     * Signal handler callback for USR2, pauses processing of new jobs.
     */
    public function pauseProcessing()
    {
        $this->paused = true;
    }

    /**
     * Signal handler callback for CONT, resumes worker allowing it to pick
     * up new jobs.
     */
    public function unPauseProcessing()
    {
        $this->paused = false;
    }

    /**
     * Schedule a worker for shutdown. Will finish processing the current job
     * and when the timeout interval is reached, the worker will shut down.
     */
    public function shutdown()
    {
        $this->shutdown = true;
    }

    /**
     * Force an immediate shutdown of the worker, killing any child jobs
     * currently running.
     */
    public function shutdownNow()
    {
        $this->shutdown();
    }

    /**
     * Tell Redis which job we're currently working on.
     *
     * @param object $item Resque_Job instance containing the job we're working on.
     */
    public function workingOn($item)
    {
        $this->working = true;
        $data = json_encode([
            'queue' => 'schedule',
            'run_at' => strftime('%a %b %d %H:%M:%S %Z %Y'),
            'payload' => $item
        ]);
        $this->resqueInstance->redis->set('worker-scheduler:' . $this, $data);
    }

    /**
     * Notify Redis that we've finished working on a job, clearing the working
     * state and incrementing the job stats.
     */
    public function doneWorking()
    {
        $this->currentJob = null;
        $this->working = false;
        $this->resqueInstance->stats->incr('processed:' . (string)$this);
        $this->resqueInstance->redis->del('worker-scheduler:' . (string)$this);
    }

    /**
     * @return boolean get for the private attribute
     */
    public function getWorking()
    {
        return $this->working;
    }

    /**
     * Return an object describing the job this worker is currently working on.
     *
     * @return object|array Object with details of current job.
     */
    public function job()
    {
        $job = $this->resqueInstance->redis->get('worker-scheduler:' . $this);
        if (!$job) {
            return [];
        } else {
            return json_decode($job, true);
        }
    }

    /**
     * Return all workers known to Resque as instantiated instances.
     * @return array
     */
    public static function all(ResqueScheduler $resqueInst)
    {
        $workers = $resqueInst->redis->smembers('worker-schedulers');
        if (!is_array($workers)) {
            $workers = [];
        }

        $instances = [];
        foreach ($workers as $workerId) {
            $instances[] = self::find($resqueInst, $workerId);
        }
        return $instances;
    }

    /**
     * Given a worker ID, check if it is registered/valid.
     *
     * @param string $workerId ID of the worker.
     * @return boolean True if the worker exists, false if not.
     */
    public function exists($workerId)
    {
        return (bool)$this->resqueInstance->redis->sismember('worker-schedulers', $workerId);
    }

    /**
     * Update the status of the current worker process.
     *
     * On supported systems (with the PECL proctitle module installed), update
     * the name of the currently running process to indicate the current state
     * of a worker.
     *
     * @param string $status The updated process title.
     */
    protected function updateProcLine($status)
    {
        $processTitle = 'resque-scheduler-' . $this . ' ' . Resque::VERSION . ': ' . $status;
        if (function_exists('cli_set_process_title') && PHP_OS !== 'Darwin') {
            cli_set_process_title($processTitle);
        } else {
            if (function_exists('setproctitle')) {
                setproctitle($processTitle);
            }
        }
    }

    /**
     * Output a given log message to STDOUT.
     *
     * @param string $message Message to output.
     */
    public function log($message)
    {
        if ($this->logLevel == self::LOG_NORMAL) {
            fwrite(STDOUT, "*** " . $message . "\n");
        } else {
            if ($this->logLevel == self::LOG_VERBOSE) {
                fwrite(STDOUT, "** [" . strftime('%T %Y-%m-%d') . "] " . $message . "\n");
            }
        }
    }

    /**
     * Get a statistic belonging to this worker.
     *
     * @param string $stat Statistic to fetch.
     * @return int Statistic value.
     */
    public function getStat($stat)
    {
        return $this->resqueInstance->stats->get($stat . ':' . $this);
    }

    /**
     * Generate a string representation of this worker.
     *
     * @return string String identifier for this worker instance.
     */
    public function __toString()
    {
        return $this->id;
    }
}