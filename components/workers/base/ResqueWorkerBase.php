<?php

namespace spiritdead\resque\components\workers\base;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\controllers\ResqueJobStatus;
use spiritdead\resque\exceptions\ResqueJobForceExitException;
use spiritdead\resque\helpers\ResqueLog;
use spiritdead\resque\Resque;

class ResqueWorkerBase
{
    /**
     * DEFAULT INTERVAL WORK
     */
    const DEFAULT_INTERVAL = 5;

    /**
     * @var ResqueLog Logging object that implements the PSR-3 LoggerInterface
     */
    public $logger;

    /**
     * @var array Array of all associated queues for this worker.
     */
    protected $queues = [];

    /**
     * @var null|Resque
     */
    protected $resqueInstance;

    /**
     * @var string The hostname of this worker.
     */
    protected $hostname;

    /**
     * @var boolean True if on the next iteration, the worker should shutdown.
     */
    protected $shutdown = false;

    /**
     * @var boolean True if this worker is paused.
     */
    protected $paused = false;

    /**
     * @var string String identifying this worker.
     */
    protected $id;

    /**
     * @var ResqueJobBase Current job, if any, being processed by this worker.
     */
    protected $currentJob = null;

    /**
     * @var boolean for display if this worker is working or not
     */
    protected $working = false;

    /**
     * @var int Process ID of child worker processes.
     */
    protected $child = null;

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
    public function __construct(Resque $resqueInst, $queues)
    {
        $this->logger = new ResqueLog();

        if (!is_array($queues)) {
            $queues = explode(',', $queues);
        }

        $this->queues = $queues;
        $this->hostname = php_uname('n');
        $this->resqueInstance = $resqueInst;

        $this->id = $this->hostname . ':' . getmypid() . ':' . implode(',', $this->queues);
    }

    /**
     * Return all workers known to Resque as instantiated instances.
     * @return null|ResqueWorkerBase[]|ResqueWorkerInterface[]
     */
    public static function all(Resque $resqueInst)
    {
        $workers = $resqueInst->redis->smembers('workers');
        if (!is_array($workers)) {
            $workers = [];
        }

        $workers = [];
        foreach ($workers as $workerId) {
            $workers[] = self::find($resqueInst, $workerId);
        }
        return $workers;
    }

    /**
     * Given a worker ID, check if it is registered/valid.
     *
     * @param string $workerId ID of the worker.
     * @return boolean True if the worker exists, false if not.
     */
    public function exists($workerId)
    {
        return (bool)$this->resqueInstance->redis->sismember('workers', $workerId);
    }

    /**
     * Given a worker ID, find it and return an instantiated worker class for it.
     *
     * @param string $workerId The ID of the worker.
     * @return boolean|ResqueWorkerBase|ResqueWorkerInterface Instance of the worker. False if the worker does not exist.
     */
    public static function find(Resque $resqueInst, $workerId)
    {
        if (!self::exists($workerId) || false === strpos($workerId, ":")) {
            return false;
        }

        list($hostname, $pid, $queues) = explode(':', $workerId, 3);
        $queues = explode(',', $queues);
        $worker = new self($resqueInst, $queues);
        $worker->id = $workerId;
        return $worker;
    }

    /**
     * @param  bool $blocking
     * @param  int $timeout
     * @return object|boolean Instance of Resque_Job if a job is found, false if not.
     */
    public function reserve($blocking = false, $timeout = null)
    {
        $queues = $this->queues();
        if (!is_array($queues)) {
            return false;
        }

        if ($blocking === true) {
            $job = ResqueJobBase::reserveBlocking($this->resqueInstance, $queues, $timeout);
            if ($job) {
                $this->logger->log(LogLevel::INFO, 'Found job on {queue}', ['queue' => $job->queue]);
                return $job;
            }
        } else {
            foreach ($queues as $queue) {
                $this->logger->log(LogLevel::INFO, 'Checking {queue} for jobs', ['queue' => $queue]);
                $job = ResqueJobBase::reserve($this->resqueInstance, $queue);
                if ($job) {
                    $this->logger->log(LogLevel::INFO, 'Found job on {queue}', ['queue' => $job->queue]);
                    return $job;
                }
            }
        }

        return false;
    }

    /**
     * Return an array containing all of the queues that this worker should use
     * when searching for jobs.
     *
     * If * is found in the list of queues, every queue will be searched in
     * alphabetic order. (@see $fetch)
     *
     * @param boolean $fetch If true, and the queue is set to *, will fetch
     * all queue names from redis.
     * @return array Array of associated queues.
     */
    public function queues($fetch = true)
    {
        if (!in_array('*', $this->queues) || $fetch == false) {
            return $this->queues;
        }

        $queues = $this->resqueInstance->queues();
        sort($queues);
        return $queues;
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
     * On supported systems (with the PECL proctitle module installed), update
     * the name of the currently running process to indicate the current state
     * of a worker.
     *
     * @param string $status The updated process title.
     */
    protected function updateProcLine($status)
    {
        $processTitle = 'resque-worker-' . $this . ' ' . Resque::VERSION . ': ' . $status;
        if (function_exists('cli_set_process_title') && PHP_OS !== 'Darwin') {
            cli_set_process_title($processTitle);
        } else {
            if (function_exists('setproctitle')) {
                setproctitle($processTitle);
            }
        }
    }

    /**
     * Register signal handlers that a worker should respond to.
     *
     * TERM: Shutdown immediately and stop processing jobs.
     * INT: Shutdown immediately and stop processing jobs.
     * QUIT: Shutdown after the current job finishes processing.
     * USR1: Kill the forked child immediately and continue processing jobs.
     */
    protected function registerSigHandlers()
    {
        if (!function_exists('pcntl_signal')) {
            return;
        }

        pcntl_signal(SIGTERM, [$this, 'shutDownNow']);
        pcntl_signal(SIGINT, [$this, 'shutDownNow']);
        pcntl_signal(SIGQUIT, [$this, 'shutdown']);
        pcntl_signal(SIGUSR1, [$this, 'killChild']);
        pcntl_signal(SIGUSR2, [$this, 'pauseProcessing']);
        pcntl_signal(SIGCONT, [$this, 'unPauseProcessing']);
        $this->logger->log(LogLevel::DEBUG, 'Registered signals');
    }

    /**
     * Signal handler callback for USR2, pauses processing of new jobs.
     */
    public function pauseProcessing()
    {
        $this->logger->log(LogLevel::NOTICE, 'USR2 received; pausing job processing');
        $this->paused = true;
    }

    /**
     * Signal handler callback for CONT, resumes worker allowing it to pick
     * up new jobs.
     */
    public function unPauseProcessing()
    {
        $this->logger->log(LogLevel::NOTICE, 'CONT received; resuming job processing');
        $this->paused = false;
    }

    /**
     * Schedule a worker for shutdown. Will finish processing the current job
     * and when the timeout interval is reached, the worker will shut down.
     */
    public function shutdown()
    {
        $this->shutdown = true;
        $this->logger->log(LogLevel::NOTICE, 'Shutting down');
    }

    /**
     * Force an immediate shutdown of the worker, killing any child jobs
     * currently running.
     */
    public function shutdownNow()
    {
        $this->shutdown();
        $this->killChild();
    }

    /**
     * Kill a forked child job immediately. The job it is processing will not
     * be completed.
     */
    public function killChild()
    {
        if (!$this->child) {
            $this->logger->log(LogLevel::DEBUG, 'No child to kill.');
            return;
        }

        $this->logger->log(LogLevel::INFO, 'Killing child at {child}', ['child' => $this->child]);
        if (exec('ps -o pid,state -p ' . $this->child, $output, $returnCode) && $returnCode != 1) {
            $this->logger->log(LogLevel::DEBUG, 'Child {child} found, killing.', ['child' => $this->child]);
            posix_kill($this->child, SIGKILL);
            $this->child = null;
        } else {
            $this->logger->log(LogLevel::INFO, 'Child {child} not found, restarting.',
                ['child' => $this->child]);
            $this->shutdown();
        }
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
        $workerPids = $this->workerPids();
        $workers = self::all($this->resqueInstance);
        foreach ($workers as $worker) {
            if (is_object($worker)) {
                list($host, $pid, $queues) = explode(':', (string)$worker, 3);
                if ($host != $this->hostname || in_array($pid, $workerPids) || $pid == getmypid()) {
                    continue;
                }
                $this->logger->log(LogLevel::INFO, 'Pruning dead worker: {worker}',
                    ['worker' => (string)$worker]);
                $worker->unregisterWorker();
            }
        }
    }

    /**
     * Return an array of process IDs for all of the Resque workers currently
     * running on this machine.
     *
     * @return array Array of Resque worker process IDs.
     */
    public function workerPids()
    {
        $pids = [];
        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            exec('tasklist /v /fi "PID gt 1" /fo csv', $cmdOutput);
            foreach ($cmdOutput as $line) {
                $a = explode(',', $line);
                list(, $pids[],) = str_replace('"', '', explode(',', trim($line), 3));
            }
        } else {
            exec('ps -A -o pid,command | grep [r]esque', $cmdOutput);
            foreach ($cmdOutput as $line) {
                list($pids[],) = explode(' ', trim($line), 2);
            }
        }
        return $pids;
    }

    /**
     * Register this worker in Redis.
     */
    public function registerWorker()
    {
        $this->resqueInstance->redis->sadd('workers', (string)$this);
        $this->resqueInstance->redis->set('worker:' . (string)$this . ':started', strftime('%a %b %d %H:%M:%S %Z %Y'));
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     */
    public function unregisterWorker()
    {
        if (is_object($this->currentJob)) {
            $this->currentJob->fail(new ResqueJobForceExitException());
        }

        $id = (string)$this;
        $this->resqueInstance->redis->srem('workers', $id);
        $this->resqueInstance->redis->del('worker:' . $id);
        $this->resqueInstance->redis->del('worker:' . $id . ':started');
        $this->resqueInstance->stats->clear('processed:' . $id);
        $this->resqueInstance->stats->clear('failed:' . $id);
    }

    /**
     * Tell Redis which job we're currently working on.
     *
     * @param ResqueJobBase $job instance containing the job we're working on.
     */
    public function workingOn(ResqueJobBase $job)
    {
        $job->worker = $this;
        $this->currentJob = $job;
        $this->working = true;
        $job->status->update(ResqueJobStatus::STATUS_RUNNING);
        $data = json_encode([
            'queue' => $job->queue,
            'run_at' => strftime('%a %b %d %H:%M:%S %Z %Y'),
            'payload' => $job->payload
        ]);
        $this->resqueInstance->redis->set('worker:' . $job->worker, $data);
    }

    /**
     * @return boolean get for the private attribute
     */
    public function getWorking()
    {
        return $this->working;
    }

    /**
     * Notify Redis that we've finished working on a job, clearing the working
     * state and incrementing the job stats.
     */
    public function doneWorking()
    {
        $this->currentJob = null;
        $this->working = false;
        $this->resqueInstance->stats->incr('processed');
        $this->resqueInstance->stats->incr('processed:' . (string)$this);
        $this->resqueInstance->redis->del('worker:' . (string)$this);
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
}