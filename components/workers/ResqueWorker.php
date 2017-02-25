<?php

namespace spiritdead\resque\components\workers;

use Psr\Log\LogLevel;
use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\components\workers\base\ResqueWorkerBase;
use spiritdead\resque\components\workers\base\ResqueWorkerInterface;
use spiritdead\resque\controllers\ResqueJobStatus;
use spiritdead\resque\exceptions\base\ResqueException;
use spiritdead\resque\exceptions\ResqueJobForceExitException;

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
                        ['interval' => $interval]);
                    $this->updateProcLine('Waiting for ' . implode(',',
                            $this->queues) . ' with blocking timeout ' . $interval);
                } else {
                    $this->updateProcLine('Waiting for ' . implode(',', $this->queues) . ' with interval ' . $interval);
                }

                $job = $this->reserve($blocking, $interval);
            }

            if (!$job) {
                // For an interval of 0, break now - helps with unit testing etc
                if ($interval == 0) {
                    break;
                }

                if ($blocking === false) {
                    // If no job was found, we sleep for $interval before continuing and checking again
                    $this->logger->log(LogLevel::INFO, 'Sleeping for {interval}', ['interval' => $interval]);
                    if ($this->paused) {
                        $this->updateProcLine('Paused');
                    } else {
                        $this->updateProcLine('Waiting for ' . implode(',', $this->queues));
                    }

                    usleep($interval * 1000000);
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
            $job->perform();
        } catch (ResqueException $e) {
            $this->logger->log(LogLevel::CRITICAL, '{job} has failed {stack}', ['job' => $job, 'stack' => $e]);
            $job->fail($e);
            return;
        }

        $job->status->update(ResqueJobStatus::STATUS_COMPLETE);
        $this->logger->log(LogLevel::NOTICE, '{job} has finished', ['job' => $job]);
    }
}