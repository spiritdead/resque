<?php

namespace spiritdead\resque\components\jobs;

use spiritdead\resque\components\jobs\base\ResqueJobBase;

/**
 * Class ResqueJob
 * @package spiritdead\resque\components\jobs
 */
class ResqueJob
{
    /**
     * @var array
     */
    public $args = [];

    /**
     * @var ResqueJobBase
     */
    public $job;

    /**
     * @var null
     */
    public $result = null;
}