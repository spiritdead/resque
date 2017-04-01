<?php

namespace spiritdead\resque\components\workers\base;

use Psr\Log\LogLevel;
use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\helpers\ResqueLog;
use spiritdead\resque\Resque;

abstract class ResqueWorkerBase
{
    // Abstract functions
    abstract protected function work($interval = ResqueWorkerBase::DEFAULT_INTERVAL, $blocking = false);

    abstract protected function workerName();

    abstract protected function pruneDeadWorkers();

    abstract protected function perform(ResqueJobBase $job);

    abstract public function registerWorker();

    abstract public function unregisterWorker();

    abstract protected function workingOn($job);

    abstract protected function doneWorking();

    abstract protected function getWorking();

    abstract public function getStartTime();

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
     */
    public function __construct(Resque $resqueInst, $queues = '*')
    {
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
        $this->resqueInstance->events->trigger('beforeFirstFork', $this);
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
}