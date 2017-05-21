<?php

namespace spiritdead\resque\plugins\schedule;

use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\exceptions\base\ResqueException;
use spiritdead\resque\exceptions\ResqueSchedulerInvalidTimestampException;
use spiritdead\resque\Resque;

/**
 * Class ResqueScheduler
 * @package spiritdead\resque\plugins
 */
class ResqueScheduler extends Resque
{
    /**
     * Enqueue a job in a given number of seconds from now.
     *
     * Identical to Resque::enqueue, however the first argument is the number
     * of seconds before the job should be executed.
     *
     * @param int $in Number of seconds from now when the job should be executed.
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     */
    public function enqueueIn($in, $queue, $class, array $args = [])
    {
        $this->enqueueAt(time() + $in, $queue, $class, $args);
    }

    /**
     * Enqueue a job for execution at a given timestamp.
     *
     * Identical to Resque::enqueue, however the first argument is a timestamp
     * (either UNIX timestamp in integer format or an instance of the DateTime
     * class in PHP).
     *
     * @param \DateTime|int $at Instance of PHP DateTime object or int of UNIX timestamp.
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     */
    public function enqueueAt($at, $queue, $class, $args = [])
    {
        $this->validateJob($class, $queue);

        $job = $this->jobToHash($queue, $class, $args);
        $this->delayedPush($at, $job);

        $this->events->trigger('afterSchedule', [
            'at' => $at,
            'queue' => $queue,
            'class' => $class,
            'args' => $args,
        ]);
    }

    /**
     * Directly append an item to the delayed queue schedule.
     *
     * @param \DateTime|int $timestamp Timestamp job is scheduled to be run at.
     * @param array $item Hash of item to be pushed to schedule.
     */
    public function delayedPush($timestamp, $item)
    {
        $timestamp = $this->getTimestamp($timestamp);
        $this->redis->rpush('delayed:' . $timestamp, json_encode($item));
        $this->redis->zadd('delayed_queue_schedule', $timestamp, $timestamp);
    }

    /**
     * Get the total number of jobs in the delayed schedule.
     *
     * @return int Number of scheduled jobs.
     */
    public function getDelayedQueueScheduleSize()
    {
        return (int)$this->redis->zcard('delayed_queue_schedule');
    }

    /**
     * Get the number of jobs for a given timestamp in the delayed schedule.
     *
     * @param \DateTime|int $timestamp Timestamp
     * @return int Number of scheduled jobs.
     */
    public function getDelayedTimestampSize($timestamp)
    {
        $timestamp = $this->toTimestamp($timestamp);
        return $this->redis->llen('delayed:' . $timestamp, $timestamp);
    }

    /**
     * Remove a delayed job from the queue
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * also, this is an expensive operation because all delayed keys have tobe
     * searched
     *
     * @param $queue
     * @param $class
     * @param $args
     * @return int number of jobs that were removed
     */
    public function removeDelayed($queue, $class, $args)
    {
        $destroyed = 0;
        $item = json_encode($this->jobToHash($queue, $class, $args));

        foreach ($this->redis->keys('delayed:*') as $key) {
            $key = $this->redis->removePrefix($key);
            $destroyed += $this->redis->lrem($key, 0, $item);
        }

        return $destroyed;
    }

    /**
     * removed a delayed job queued for a specific timestamp
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * @param $timestamp
     * @param $queue
     * @param $class
     * @param $args
     * @return mixed
     */
    public function removeDelayedJobFromTimestamp($timestamp, $queue, $class, $args)
    {
        $key = 'delayed:' . $this->getTimestamp($timestamp);
        $item = json_encode($this->jobToHash($queue, $class, $args));
        $count = $this->redis->lrem($key, 0, $item);
        $this->cleanupTimestamp($key, $timestamp);

        return $count;
    }

    /**
     * Generate hash of all job properties to be saved in the scheduled queue.
     *
     * @param string $queue Name of the queue the job will be placed on.
     * @param string $class Name of the job class.
     * @param array $args Array of job arguments.
     * @return array
     */
    private function jobToHash($queue, $class, $args)
    {
        return [
            'class' => $class,
            'args' => [$args],
            'queue' => $queue,
        ];
    }

    /**
     * If there are no jobs for a given key/timestamp, delete references to it.
     *
     * Used internally to remove empty delayed: items in Redis when there are
     * no more jobs left to run at that timestamp.
     *
     * @param string $key Key to count number of items at.
     * @param int $timestamp Matching timestamp for $key.
     */
    private function cleanupTimestamp($key, $timestamp)
    {
        $timestamp = $this->getTimestamp($timestamp);

        if ($this->redis->llen($key) == 0) {
            $this->redis->del($key);
            $this->redis->zrem('delayed_queue_schedule', $timestamp);
        }
    }

    /**
     * Convert a timestamp in some format in to a unix timestamp as an integer.
     *
     * @param \DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     * @return int Timestamp
     * @throws ResqueSchedulerInvalidTimestampException
     */
    private function getTimestamp($timestamp)
    {
        if ($timestamp instanceof \DateTime) {
            $timestamp = $timestamp->getTimestamp();
        }

        if ((int)$timestamp != $timestamp) {
            throw new ResqueSchedulerInvalidTimestampException(
                'The supplied timestamp value could not be converted to an integer.'
            );
        }

        return (int)$timestamp;
    }

    /**
     * Find the first timestamp in the delayed schedule before/including the timestamp.
     *
     * Will find and return the first timestamp upto and including the given
     * timestamp. This is the heart of the ResqueScheduler that will make sure
     * that any jobs scheduled for the past when the worker wasn't running are
     * also queued up.
     *
     * @param \DateTime|int $at Instance of DateTime or UNIX timestamp.
     *                                Defaults to now.
     * @return int|false UNIX timestamp, or false if nothing to run.
     */
    public function nextDelayedTimestamp($at = null)
    {
        if ($at === null) {
            $at = time();
        } else {
            $at = $this->getTimestamp($at);
        }

        $items = $this->redis->zrangebyscore('delayed_queue_schedule', '-inf', $at, ['limit' => [0, 1]]);
        if (!empty($items)) {
            return $items[0];
        }

        return false;
    }

    /**
     * Pop a job off the delayed queue for a given timestamp.
     *
     * @param \DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     * @return ResqueJobBase Matching job at timestamp.
     */
    public function nextItemForTimestamp($timestamp)
    {
        $timestamp = $this->getTimestamp($timestamp);
        $key = 'delayed:' . $timestamp;
        $item = json_decode($this->redis->lpop($key), true);
        $this->cleanupTimestamp($key, $timestamp);
        if(isset($item) && is_array($item)) {
            $payload = [
                'class' => $item['class'],
                'args' => $item['args']
            ];
            return new ResqueJobBase($this, $item['queue'],$payload);
        }
        return null;
    }

    /**
     * Ensure that supplied job class/queue is valid.
     *
     * @param string $class Name of job class.
     * @param string $queue Name of queue.
     * @throws ResqueException
     *
     * @return boolean
     */
    private function validateJob($class, $queue)
    {
        if (empty($class)) {
            throw new ResqueException('Jobs must be given a class.');
        } else {
            if (empty($queue)) {
                throw new ResqueException('Jobs must be put in a queue.');
            }
        }
        return true;
    }
}
