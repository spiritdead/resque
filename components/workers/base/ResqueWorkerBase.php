<?php

namespace spiritdead\resque\components\workers\base;

use Psr\Log\LogLevel;
use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\controllers\ResqueJobStatus;
use spiritdead\resque\exceptions\ResqueJobForceExitException;
use spiritdead\resque\exceptions\ResqueWorkerInvalidException;
use spiritdead\resque\helpers\ResqueLog;
use spiritdead\resque\Resque;

abstract class ResqueWorkerBase
{
    // Abstract functions
    abstract protected function work($interval = ResqueWorkerBase::DEFAULT_INTERVAL, $blocking = false);

    abstract protected function workerName();

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
     * @var boolean True if on the next iteration, the worker should shutdown.
     */
    protected $shutdown = false;

    /**
     * @var boolean True if this worker is paused.
     */
    protected $paused = false;

    /**
     * @var string identifying this worker.
     */
    protected $id;

    /**
     * @var integer process id in the server
     */
    protected $pid;

    /**
     * @var ResqueJobBase Current job, if any, being processed by this worker.
     */
    public $currentJob = null;

    /**
     * @var boolean for display if this worker is working or not
     */
    protected $working = false;

    /**
     * @var int Process ID of child worker processes.
     */
    protected $child = null;

    /**
     * @var int Interval to sleep for between checking schedules.
     */
    protected $interval = self::DEFAULT_INTERVAL;

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
     * @throws ResqueWorkerInvalidException
     */
    public function __construct(Resque $resqueInst, $queues = '*')
    {
        if (!$this instanceof ResqueWorkerInterface) {
            throw new ResqueWorkerInvalidException('The worker have to implement the base interface');
        }

        $this->logger = new ResqueLog();
        if (!is_array($queues)) {
            $queues = explode(',', $queues);
        }
        $this->queues = $queues;
        $this->resqueInstance = $resqueInst;
        $this->id = $this->resqueInstance->backend->namespaceWorkers . ':' . getmypid() . ':' . implode(',',
                $this->queues);
        $this->pid = getmypid();
    }

    /**
     * Method for regenerate worker from the current ID saved in the redis and the instance in the server
     * @param $workerInstance
     * @return boolean // Success or Fail
     */
    public function restore($workerInstance)
    {
        list($hostname, $pid, $queues) = explode(':', $workerInstance, 3);
        if (!is_array($queues)) {
            $queues = explode(',', $queues);
        }

        $this->queues = $queues;
        $this->pid = $pid;
        $this->id = $workerInstance; //regenerate worker
        $data = $this->resqueInstance->redis->get($this->workerName() . ':' . $workerInstance);
        if ($data !== false) {
            $data = json_decode($data, true);
            $this->currentJob = new ResqueJobBase($this->resqueInstance, $data['queue'], $data['payload'], true);
        }
        $workerPids = self::workerPids();
        if (!in_array($pid, $workerPids)) {
            $this->unregisterWorker();
            return false;
        }
        return true;
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
    protected function shutdown()
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
     * Return an array of process IDs for all of the Resque workers currently
     * running on this machine.
     *
     * @return array Array of Resque worker process IDs.
     */
    public static function workerPids()
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
        $this->resqueInstance->redis->del($this->workerName() . ':' . (string)$this);
    }

    /**
     * Tell Redis which job we're currently working on.
     *
     * @param ResqueJobBase $job instance containing the job we're working on.
     * @param string $queue queue of work
     */
    public function workingOn($job, $queue = null)
    {
        $job->worker = $this;
        $this->currentJob = $job;
        $this->working = true;
        if(isset($job->status)) {
            $job->status->update(ResqueJobStatus::STATUS_RUNNING);
        }
        $data = json_encode([
            'queue' => isset($queue) ? $queue : $job->queue,
            'run_at' => strtotime('now UTC'),
            'payload' => $job->payload
        ]);
        $this->resqueInstance->redis->set($this->workerName() . ':' . $job->worker, $data);
    }

    /**
     * Time start to work
     * @return int
     */
    public function getStartTime()
    {
        return $this->resqueInstance->redis->get($this->workerName() . ':' . $this->id . ':started');
    }

    /**
     * Register this worker in Redis.
     */
    public function registerWorker()
    {
        $this->resqueInstance->redis->sadd($this->workerName() . 's', (string)$this);
        $this->resqueInstance->redis->set($this->workerName() . ':' . (string)$this . ':started', strtotime('now UTC'));
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     */
    public function unregisterWorker()
    {
        if (is_object($this->currentJob)) {
            $this->currentJob->fail(new ResqueJobForceExitException());
        }

        $this->resqueInstance->redis->srem($this->workerName() . 's', $this->id);
        $this->resqueInstance->redis->del($this->workerName() . ':' . $this->id);
        $this->resqueInstance->redis->del($this->workerName() . ':' . $this->id . ':started');
        $this->resqueInstance->stats->clear('processed:' . $this->id);
        $this->resqueInstance->stats->clear('failed:' . $this->id);
    }

    /**
     * Perform necessary actions to start a worker.
     */
    public function startup()
    {
        $this->pruneDeadWorkers();
        $this->registerWorker();
        $this->registerSigHandlers();
        $this->resqueInstance->events->trigger('beforeFirstFork', $this);
    }

    /**
     * Given a worker ID, find it and return an instantiated worker class for it.
     *
     * @param Resque $resqueInst
     * @param string $workerId The ID of the worker.
     * @return null|ResqueWorkerBase|ResqueWorkerInterface Instance of the worker. False if the worker does not exist.
     * @throws ResqueWorkerInvalidException
     */
    public static function find($resqueInst, $workerId)
    {
        if (!self::exists($resqueInst, $workerId) || false === strpos($workerId, ":")) {
            return null;
        }

        $worker = new static($resqueInst);
        if ($worker->restore($workerId)) {
            return $worker;
        }
        return null;
    }

    /**
     * @param Resque $resqueInst
     * Return all workers known to Resque as instantiated instances.
     * @return null|ResqueWorkerBase[]|ResqueWorkerInterface[]
     * @throws ResqueWorkerInvalidException
     */
    public static function all($resqueInst)
    {
        $class = get_called_class();
        $workers = [];
        if (isset($class) && !empty($class) && method_exists($class, 'workerName')) {
            $workerName = call_user_func($class . '::workerName');
            $workersRaw = $resqueInst->redis->smembers($workerName . 's');
            if (is_array($workersRaw) && count($workersRaw) > 0) {
                foreach ($workersRaw as $workerId) {
                    $worker = self::find($resqueInst, $workerId);
                    if (isset($worker)) {
                        $workers[] = $worker;
                    }
                }
            }
            return $workers;
        } else {
            throw new ResqueWorkerInvalidException(['method workerName not exist in child class']);
        }
    }

    /**
     * Given a worker ID, check if it is registered/valid.
     *
     * @param Resque $resqueInst instance of resque
     * @param string $workerId ID of the worker.
     * @return boolean True if the worker exists, false if not.
     * @throws ResqueWorkerInvalidException
     */
    public static function exists($resqueInst, $workerId)
    {
        $class = get_called_class();
        if (isset($class) && !empty($class) && method_exists($class, 'workerName')) {
            $workerName = call_user_func($class . '::workerName');
            return (bool)$resqueInst->redis->sismember($workerName . 's', $workerId);
        } else {
            throw new ResqueWorkerInvalidException(['method workerName not exist in child class']);
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
    protected function pruneDeadWorkers()
    {
        $workerPids = self::workerPids();
        $workers = self::all($this->resqueInstance);
        foreach ($workers as $worker) {
            if (is_object($worker)) {
                list($host, $pid, $queues) = explode(':', (string)$worker, 3);
                if ($host != $this->resqueInstance->backend->namespaceWorkers || in_array($pid,
                        $workerPids) || $pid == getmypid()
                ) {
                    continue;
                }
                $this->logger->log(LogLevel::INFO, 'Pruning dead worker: {worker}',
                    ['worker' => (string)$worker]);
                $worker->unregisterWorker();
            }
        }
    }
}