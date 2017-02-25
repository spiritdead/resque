<?php

namespace spiritdead\resque\helpers;

use spiritdead\resque\Resque;

/**
 * Class Resque_Stat
 * @package spiritdead\resque\helpers
 */
class ResqueStat
{
    /**
     * @var null|Resque
     */
    private $resqueInstance = null;

    public function __construct(Resque $resqueInst)
    {
        $this->resqueInstance = $resqueInst;
    }

    /**
     * Get the value of the supplied statistic counter for the specified statistic.
     *
     * @param string $stat The name of the statistic to get the stats for.
     * @return mixed Value of the statistic.
     */
    public function get($stat)
    {
        return (int)$this->resqueInstance->redis->get('stat:' . $stat);
    }

    /**
     * Increment the value of the specified statistic by a certain amount (default is 1)
     *
     * @param string $stat The name of the statistic to increment.
     * @param int $by The amount to increment the statistic by.
     * @return boolean True if successful, false if not.
     */
    public function incr($stat, $by = 1)
    {
        return (bool)$this->resqueInstance->redis->incrby('stat:' . $stat, $by);
    }

    /**
     * Decrement the value of the specified statistic by a certain amount (default is 1)
     *
     * @param string $stat The name of the statistic to decrement.
     * @param int $by The amount to decrement the statistic by.
     * @return boolean True if successful, false if not.
     */
    public function decr($stat, $by = 1)
    {
        return (bool)$this->resqueInstance->redis->decrby('stat:' . $stat, $by);
    }

    /**
     * Delete a statistic with the given name.
     *
     * @param string $stat The name of the statistic to delete.
     * @return boolean True if successful, false if not.
     */
    public function clear($stat)
    {
        return (bool)$this->resqueInstance->redis->del('stat:' . $stat);
    }
}