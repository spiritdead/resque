<?php

namespace spiritdead\resque;

use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\controllers\ResqueRedis;
use spiritdead\resque\exceptions\ResqueJobCreateException;
use spiritdead\resque\helpers\ResqueEvent;
use spiritdead\resque\helpers\ResqueHelper;
use spiritdead\resque\helpers\ResqueStat;
use spiritdead\resque\models\ResqueBackend;

/**
 * Class Resque
 * @package spiritdead\resque
 */
class Resque
{
    /**
     * Version of the resque
     */
    const VERSION = '1.0.5';

    /**
     * @var null|ResqueRedis
     */
    private static $redisGlobal = null;

    /**
     * @var null|ResqueBackend
     */
    public $backend = null;

    /**
     * @var null|ResqueEvent
     */
    public $events = null;

    /**
     * @var null|ResqueStat
     */
    public $stats = null;

    /**
     * @var null|ResqueRedis
     */
    public $redis = null;

    /**
     * Resque constructor.
     * @param ResqueBackend|null $backend
     */
    public function __construct(ResqueBackend $backend = null)
    {
        if ($backend === null) {
            $this->backend = new ResqueBackend();
        } else {
            $this->backend = $backend;
        }
        $this->events = new ResqueEvent();
        if (self::$redisGlobal === null) {
            self::$redisGlobal = new ResqueRedis($this->backend);
        }
        $this->redis = self::$redisGlobal;
        $this->stats = new ResqueStat($this);
    }

    /**
     * fork() helper method for php-resque that handles issues PHP socket
     * and phpredis have with passing around sockets between child/parent
     * processes.
     *
     * Will close connection to Redis before forking.
     *
     * @return int Return vars as per pcntl_fork(). False if pcntl_fork is unavailable
     */
    public function fork()
    {
        if (!function_exists('pcntl_fork')) {
            return false;
        }

        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new \RuntimeException('Unable to fork child worker.');
        }
        return $pid;
    }

    /**
     * Push a job to the end of a specific queue. If the queue does not
     * exist, then create it as well.
     *
     * @param string $queue The name of the queue to add the job to.
     * @param array $payload Job description as an array to be JSON encoded.
     *
     * @return boolean
     */
    public function push($queue, $payload)
    {
        $encodedItem = json_encode($payload);
        if ($encodedItem === false) {
            return false;
        }
        $this->redis->sadd('queues', $queue);
        $length = $this->redis->rpush('queue:' . $queue, $encodedItem);
        if ($length < 1) {
            return false;
        }
        return true;
    }

    /**
     * Pop an item off the end of the specified queue, decode it and
     * return it.
     *
     * @param string $queue The name of the queue to fetch an item from.
     * @return array Decoded item from the queue.
     */
    public function pop($queue)
    {
        $item = $this->redis->lpop('queue:' . $queue);

        if (!$item) {
            return null;
        }
        return json_decode($item, true);
    }

    /**
     * Pop an item off the end of the specified queues, using blocking list pop,
     * decode it and return it.
     *
     * @param array $queues
     * @param int $timeout
     * @return null|array   Decoded item from the queue.
     */
    public function blpop(array $queues, $timeout)
    {
        $list = [];
        foreach ($queues AS $queue) {
            $list[] = 'queue:' . $queue;
        }

        $item = $this->redis->blpop($list, (int)$timeout);

        if (!$item) {
            return null;
        }

        /**
         * Normally the Resque_Redis class returns queue names without the prefix
         * But the blpop is a bit different. It returns the name as prefix:queue:name
         * So we need to strip off the prefix:queue: part
         */
        $queue = substr($item[0], strlen($this->redis->getPrefix() . 'queue:'));

        return [
            'queue' => $queue,
            'payload' => json_decode($item[1], true)
        ];
    }

    /**
     * Return the size (number of pending jobs) of the specified queue.
     *
     * @param string $queue name of the queue to be checked for pending jobs
     *
     * @return int The size of the queue.
     */
    public function size($queue)
    {
        return $this->redis->llen('queue:' . $queue);
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     * @param boolean $trackStatus Set to true to be able to monitor the status of a job.
     *
     * @return string|boolean Job ID when the job was created, false if creation was cancelled due to beforeEnqueue
     */
    public function enqueue($queue, $class, $args = null, $trackStatus = false)
    {
        $id = ResqueHelper::generateJobId();
        $hookParams = [
            'class' => $class,
            'args' => $args,
            'queue' => $queue,
            'id' => $id,
        ];

        try {
            $this->events->trigger('beforeEnqueue', $hookParams);

            $job = new ResqueJobBase($this, $queue);
            $job->create($class, $args, $trackStatus, $id);
            $this->push($queue, $job->payload);

            $this->events->trigger('afterEnqueue', $hookParams);
        } catch (ResqueJobCreateException $e) {
            return false;
        }
        return $id;
    }

    /**
     * Remove items of the specified queue
     *
     * @param string $queue The name of the queue to fetch an item from.
     * @param array $items
     * @return integer number of deleted items
     */
    public function dequeue($queue, $items = [])
    {
        if (count($items) > 0) {
            return $this->removeItems($queue, $items);
        } else {
            return $this->removeList($queue);
        }
    }

    /**
     * Get an array of all known queues.
     *
     * @return array Array of queues.
     */
    public function queues()
    {
        $queues = $this->redis->smembers('queues');
        if (!is_array($queues)) {
            $queues = [];
        }
        return $queues;
    }

    /**
     * Remove specified queue
     *
     * @param string $queue The name of the queue to remove.
     * @return integer Number of deleted items
     */
    public function removeQueue($queue)
    {
        $num = $this->removeList($queue);
        $this->redis->srem('queues', $queue);
        return $num;
    }

    /**
     * Reserve and return the next available job in the specified queue.
     *
     * @param string $queue Queue to fetch next available job from.
     * @return ResqueJobBase Instance of Resque_Job to be processed, false if none or error.
     */
    public function reserve($queue)
    {
        return ResqueJobBase::reserve($this, $queue);
    }

    /**
     * Remove Items from the queue
     * Safely moving each item to a temporary queue before processing it
     * If the Job matches, counts otherwise puts it in a requeue_queue
     * which at the end eventually be copied back into the original queue
     *
     * @private
     *
     * @param string $queue The name of the queue
     * @param array $items
     * @return integer number of deleted items
     */
    private function removeItems($queue, $items = [])
    {
        $counter = 0;
        $originalQueue = 'queue:' . $queue;
        $tempQueue = $originalQueue . ':temp:' . time();
        $requeueQueue = $tempQueue . ':requeue';

        // move each item from original queue to temp queue and process it
        $finished = false;
        while (!$finished) {
            $string = $this->redis->rpoplpush($originalQueue, $this->redis->getPrefix() . $tempQueue);

            if (!empty($string)) {
                if ($this->matchItem($string, $items)) {
                    $this->redis->rpop($tempQueue);
                    $counter++;
                } else {
                    $this->redis->rpoplpush($tempQueue, $this->redis->getPrefix() . $requeueQueue);
                }
            } else {
                $finished = true;
            }
        }

        // move back from temp queue to original queue
        $finished = false;
        while (!$finished) {
            $string = $this->redis->rpoplpush($requeueQueue, $this->redis->getPrefix() . $originalQueue);
            if (empty($string)) {
                $finished = true;
            }
        }

        // remove temp queue and requeue queue
        $this->redis->del($requeueQueue);
        $this->redis->del($tempQueue);

        return $counter;
    }

    /**
     * matching item
     * item can be ['class'] or ['class' => 'id'] or ['class' => {:foo => 1, :bar => 2}]
     * @private
     *
     * @params string $string redis result in json
     * @params $items
     *
     * @return bool
     */
    private function matchItem($string, $items)
    {
        $decoded = json_decode($string, true);

        foreach ($items as $key => $val) {
            # class name only  ex: item[0] = ['class']
            if (is_numeric($key)) {
                if ($decoded['class'] == $val) {
                    return true;
                }
                # class name with args , example: item[0] = ['class' => {'foo' => 1, 'bar' => 2}]
            } elseif (is_array($val)) {
                $decodedArgs = (array)$decoded['args'][0];
                if ($decoded['class'] == $key &&
                    count($decodedArgs) > 0 && count(array_diff($decodedArgs, $val)) == 0
                ) {
                    return true;
                }
                # class name with ID, example: item[0] = ['class' => 'id']
            } else {
                if ($decoded['class'] == $key && $decoded['id'] == $val) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Remove List
     *
     * @private
     *
     * @params string $queue the name of the queue
     * @return integer number of deleted items belongs to this list
     */
    private function removeList($queue)
    {
        $counter = $this->size($queue);
        $result = $this->redis->del('queue:' . $queue);
        return ($result == 1) ? $counter : 0;
    }
}