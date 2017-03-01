<?php

namespace spiritdead\resque\plugins\schedule\workers;

use Psr\Log\LogLevel;
use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\components\workers\base\ResqueWorkerBase;
use spiritdead\resque\components\workers\base\ResqueWorkerInterface;
use spiritdead\resque\plugins\schedule\ResqueScheduler;

/**
 * Class ResqueWorkerScheduler
 * @package spiritdead\resque\components\workers
 */
class ResqueWorkerScheduler extends ResqueWorkerBase implements ResqueWorkerInterface
{
    /**
     * @var null|ResqueScheduler
     */
    protected $resqueInstance;

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
    public function __construct(ResqueScheduler $resqueInst, $queues = 'delayed_queue_schedule')
    {
        parent::__construct($resqueInst, $queues);
    }

    /**
     * Return all workers known to Resque as instantiated instances.
     * @param ResqueScheduler $resqueInst
     * @return null|ResqueWorkerScheduler[]
     */
    public static function all($resqueInst)
    {
        $workersRaw = $resqueInst->redis->smembers('worker-schedulers');
        $workers = [];
        if (is_array($workersRaw) && count($workersRaw) > 0) {
            foreach ($workersRaw as $workerId) {
                $workers[] = self::find($resqueInst, $workerId);
            }
        }
        return $workers;
    }

    /**
     * Given a worker ID, find it and return an instantiated worker class for it.
     *
     * @param ResqueScheduler $resqueInst
     * @param string $workerId The ID of the worker.
     * @return boolean|ResqueWorkerBase|ResqueWorkerInterface Instance of the worker. False if the worker does not exist.
     */
    public static function find($resqueInst, $workerId)
    {
        if (!self::exists($resqueInst, $workerId) || false === strpos($workerId, ":")) {
            return false;
        }

        list($hostname, $pid, $queues) = explode(':', $workerId, 3);
        $queues = explode(',', $queues);
        $worker = new self($resqueInst, $queues);
        $worker->id = $workerId;
        return $worker;
    }

    /**
     * Given a worker ID, check if it is registered/valid.
     *
     * @param ResqueScheduler $resqueInst instance of resque
     * @param string $workerId ID of the worker.
     * @return boolean True if the worker exists, false if not.
     */
    public static function exists($resqueInst, $workerId)
    {
        return (bool)$resqueInst->redis->sismember('worker-schedulers', $workerId);
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
    public function work($interval = self::DEFAULT_INTERVAL, $blockinge = false)
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

            $this->logger->log(LogLevel::NOTICE, 'queueing ' . $item['class'] . ' in ' . $item['queue'] . ' [delayed]');
            $this->resqueInstance->events->trigger('beforeDelayedEnqueue', [
                'class' => $item['class'],
                'args' => $item['args'],
                'queue' => $item['queue']
            ]);
            $this->resqueInstance->enqueue($item['queue'], $item['class'], $item['args'][0]);

            $this->doneWorking();
        }
    }

    /**
     * Perform necessary actions to start a worker.
     */
    public function startup()
    {
        $this->pruneDeadWorkers();
        $this->registerWorker();
        parent::startup();
    }

    /**
     * Register this worker in Redis.
     */
    public function registerWorker()
    {
        $this->resqueInstance->redis->sadd('worker-schedulers', (string)$this);
        $this->resqueInstance->redis->set('worker-scheduler:' . (string)$this . ':started',
            strftime('%a %b %d %H:%M:%S %Z %Y'));
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     */
    public function unregisterWorker()
    {
        $this->resqueInstance->redis->srem('worker-schedulers', $this->id);
        $this->resqueInstance->redis->del('worker-scheduler:' . $this->id);
        $this->resqueInstance->redis->del('worker-scheduler:' . $this->id . ':started');
        $this->resqueInstance->stats->clear('processed:' . $this->id);
        $this->resqueInstance->stats->clear('failed:' . $this->id);
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
        $workerPids = parent::workerPids();
        $workers = self::all($this->resqueInstance);
        foreach ($workers as $worker) {
            if (is_object($worker)) {
                list($host, $pid, $queues) = explode(':', (string)$worker, 3);
                if ($host != $this->resqueInstance->backend->namespaceWorkers || in_array($pid, $workerPids) || $pid == getmypid()) {
                    continue;
                }
                $this->logger->log(LogLevel::INFO, 'Pruning dead worker: {worker}',
                    ['worker' => (string)$worker]);
                $worker->unregisterWorker();
            }
        }
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