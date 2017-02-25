<?php

namespace spiritdead\resque\components\jobs;

use spiritdead\resque\components\jobs\base\ResqueJobBase;

/**
 * Class ResqueJob
 * @package spiritdead\resque\components\jobs
 */
class ResqueJob extends ResqueJobBase
{
    /**
     * @var ResqueJob
     */
    public $job;
}