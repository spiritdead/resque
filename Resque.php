<?php

namespace spiritdead\resque;

use spiritdead\resque\controllers\Resque_Redis;
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
    const VERSION = '1.0';

    /**
     * @var null | Resque_Redis
     */
    public $redis = null;

    /**
     * @var null | ResqueBackend
     */
    public $backend = null;

    public function __construct(ResqueBackend $backend = null)
    {

    }

    public function redis()
    {
        if ($this->redis != null) {
            return $this->redis;
        }
        $this->redis = new Resque_Redis($this->backend);
        return $this->redis;
    }

    public function push($queue){

    }

    public function pop($queue){

    }

}