<?php

namespace spiritdead\resque\components\workers;

use Psr\Log\LogLevel;
use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\components\workers\base\ResqueWorkerBase;
use spiritdead\resque\components\workers\base\ResqueWorkerInterface;
use spiritdead\resque\controllers\ResqueJobStatus;
use spiritdead\resque\exceptions\ResqueJobPerformException;
use spiritdead\resque\Resque;
use spiritdead\resque\exceptions\ResqueJobForceExitException;

/**
 * Class ResqueWorker
 * @package spiritdead\resque\components\workers
 */
class ResqueWorker extends ResqueWorkerBase implements ResqueWorkerInterface
{
    /**
     * The primary loop for a worker which when called on an instance starts
     * the worker's life cycle.
     *
     * Queues are checked every $interval (seconds) for new jobs.
     *
     * @param int $interval How often to check for new jobs across the queues.
     */
    public function work($interval = ResqueWorkerBase::DEFAULT_INTERVAL, $blocking = false)
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

            // Attempt to find and reserve a job
            $job = false;
            if (!$this->paused) {
                if ($blocking === true) {
                    $this->logger->log(LogLevel::INFO, 'Starting blocking with timeout of {interval}',
                        ['interval' => $this->interval]);
                    $this->updateProcLine('Waiting for ' . implode(',',
                            $this->queues) . ' with blocking timeout ' . $this->interval);
                } else {
                    $this->updateProcLine('Waiting for ' . implode(',',
                            $this->queues) . ' with interval ' . $this->interval);
                }

                $job = $this->reserve($blocking, $this->interval);
            }

            if (!$job) {
                // For an interval of 0, break now - helps with unit testing etc
                if ($this->interval == 0) {
                    break;
                }

                if ($blocking === false) {
                    // If no job was found, we sleep for $this->interval before continuing and checking again
                    $this->logger->log(LogLevel::INFO, 'Sleeping for {interval}', ['interval' => $this->interval]);
                    if ($this->paused) {
                        $this->updateProcLine('Paused');
                    } else {
                        $this->updateProcLine('Waiting for ' . implode(',', $this->queues));
                    }

                    usleep($this->interval * 1000000);
                }

                continue;
            }

            $this->logger->log(LogLevel::NOTICE, 'Starting work on {job}', ['job' => $job]);
            $this->resqueInstance->events->trigger('beforeFork', $job);
            $this->workingOn($job);

            $this->child = $this->resqueInstance->fork();

            // Forked and we're the child. Run the job.
            if ($this->child === 0 || $this->child === false) {
                $status = 'Processing ' . $job->queue . ' since ' . date('d/m/Y h:i:s a');
                $this->updateProcLine($status);
                $this->logger->log(LogLevel::INFO, $status);
                $this->perform($job);
                if ($this->child === 0) {
                    exit(0);
                }
            }

            if ($this->child > 0) {
                // Parent process, sit and wait
                $status = 'Forked ' . $this->child . ' at ' . date('d/m/Y h:i:s a');
                $this->updateProcLine($status);
                $this->logger->log(LogLevel::INFO, $status);

                // Wait until the child process finishes before continuing
                pcntl_wait($status);
                $exitStatus = pcntl_wexitstatus($status);
                if ($exitStatus !== 0) {
                    $job->fail(new ResqueJobForceExitException(
                        'Job exited with exit code ' . $exitStatus
                    ));
                }
            }

            $this->child = null;
            $this->doneWorking();
        }

        $this->unregisterWorker();
    }

    /**
     * Process a single job.
     *
     * @param ResqueJobBase $job The job to be processed.
     */
    public function perform(ResqueJobBase $job)
    {
        try {
            $this->resqueInstance->events->trigger('afterFork', $job);
            $result = $job->perform();
            if ($result instanceof ResqueJobPerformException) {
                $this->logger->log(LogLevel::CRITICAL, '{job} has failed {message} at the line {line}',
                    [
                        'job' => $job,
                        'message' => $result->getMessage(),
                        'line' => $result->getLine()
                    ]);
                return;
            }
        } catch (\Exception $e) {
            $this->logger->log(LogLevel::CRITICAL, '{job} has failed {message} at the line {line}',
                [
                    'job' => $job,
                    'message' => $e->getMessage(),
                    'line' => $e->getLine()
                ]);
            return;
        }

        $job->status->update(ResqueJobStatus::STATUS_COMPLETE);
        $this->logger->log(LogLevel::NOTICE, '{job} has finished', ['job' => $job]);
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
     * Given a worker ID, find it and return an instantiated worker class for it.
     *
     * @param Resque $resqueInst
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
     * @param Resque $resqueInst
     * Return all workers known to Resque as instantiated instances.
     * @return null|ResqueWorkerBase[]|ResqueWorkerInterface[]
     */
    public static function all($resqueInst)
    {
        $workersRaw = $resqueInst->redis->smembers('workers');
        $workers = [];
        if (is_array($workersRaw) && count($workersRaw) > 0) {
            foreach ($workersRaw as $workerId) {
                $workers[] = self::find($resqueInst, $workerId);
            }
        }
        return $workers;
    }

    /**
     * Given a worker ID, check if it is registered/valid.
     *
     * @param Resque $resqueInst instance of resque
     * @param string $workerId ID of the worker.
     * @return boolean True if the worker exists, false if not.
     */
    public static function exists($resqueInst, $workerId)
    {
        return (bool)$resqueInst->redis->sismember('workers', $workerId);
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

        $this->resqueInstance->redis->srem('workers', $this->id);
        $this->resqueInstance->redis->del('worker:' . $this->id);
        $this->resqueInstance->redis->del('worker:' . $this->id . ':started');
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
     * @param ResqueJobBase $job instance containing the job we're working on.
     */
    public function workingOn($job)
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
}