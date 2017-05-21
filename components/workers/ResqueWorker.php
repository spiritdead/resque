<?php

namespace spiritdead\resque\components\workers;

use Psr\Log\LogLevel;
use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\components\workers\base\ResqueWorkerBase;
use spiritdead\resque\components\workers\base\ResqueWorkerInterface;
use spiritdead\resque\controllers\ResqueJobStatus;
use spiritdead\resque\exceptions\ResqueJobForceExitException;
use spiritdead\resque\Resque;

/**
 * Class ResqueWorker
 * @package spiritdead\resque\components\workers
 */
class ResqueWorker extends ResqueWorkerBase implements ResqueWorkerInterface
{
    /**
     * Name instances in redis
     */
    const WORKER_NAME = 'worker';

    /**
     * Instantiate a new worker, given a list of queues that it should be working
     * on. The list of queues should be supplied in the priority that they should
     * be checked for jobs (first come, first served)
     *
     * Passing a single '*' allows the worker to work on all queues in alphabetical
     * order. You can easily add new queues dynamically and have them worked on using
     * this method.
     *
     * @param Resque $resqueInst instance resque
     * @param string|array $queues String with a single queue name, array with multiple.
     */
    public function __construct(Resque $resqueInst, $queues = '*')
    {
        parent::__construct($resqueInst, $queues);
    }

    /**
     * Name instance in redis
     * @return string
     */
    public function workerName()
    {
        return self::WORKER_NAME;
    }

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
            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }
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
            if ($result instanceof \Exception) {
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
}