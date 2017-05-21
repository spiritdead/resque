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
     * Name instances in redis
     */
    const WORKER_NAME = 'worker-scheduler';

    /**
     * Name instance in redis
     * @return string
     */
    public function workerName()
    {
        return self::WORKER_NAME;
    }

    /**
     * Instantiate a new worker, given a list of queues that it should be working
     * on. The list of queues should be supplied in the priority that they should
     * be checked for jobs (first come, first served)
     *
     * Passing a single '*' allows the worker to work on all queues in alphabetical
     * order. You can easily add new queues dynamically and have them worked on using
     * this method.
     *
     * @param ResqueScheduler $resqueInst instance resque
     * @param string|array $queues String with a single queue name, array with multiple.
     */
    public function __construct(ResqueScheduler $resqueInst, $queues = 'delayed_queue_schedule')
    {
        parent::__construct($resqueInst, $queues);
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
            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }
            if ($this->shutdown) {
                break;
            }
            if (!$this->paused) {
                /** Handle delayed items for the next scheduled timestamp. */
                while (($oldestJobTimestamp = $this->resqueInstance->nextDelayedTimestamp(null)) !== false) {
                    $this->updateProcLine('Processing Delayed Items');
                    $this->enqueueDelayedItemsForTimestamp($oldestJobTimestamp);
                }
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
        $job = null;
        while ($job = $this->resqueInstance->nextItemForTimestamp($timestamp)) {
            $this->workingOn($job, 'schedule');

            $this->logger->log(LogLevel::NOTICE, 'queueing ' . $job->payload['class'] . ' in ' . $job->queue . ' [delayed]');
            $this->resqueInstance->events->trigger('beforeDelayedEnqueue', [
                'class' => $job->payload['class'],
                'args' => $job->payload['args'],
                'queue' => $job->queue
            ]);
            $this->resqueInstance->enqueue($job->queue, $job->payload['class'] , $job->payload['args'][0]);

            $this->doneWorking();
        }
    }

    /**
     * Return an object describing the job this worker is currently working on.
     *
     * @return object|array Object with details of current job.
     */
    public function job()
    {
        $job = $this->resqueInstance->redis->get(self::WORKER_NAME . ':' . $this);
        if (!$job) {
            return [];
        } else {
            return json_decode($job, true);
        }
    }
}