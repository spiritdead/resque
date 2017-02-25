<?php

namespace spiritdead\resque\components\workers\base;

use spiritdead\resque\components\jobs\base\ResqueJobBase;

/**
 * Interface ResqueWorkerInterface
 * @package spiritdead\resque\components\workers\base
 */
interface ResqueWorkerInterface
{
    public function work($interval = ResqueWorkerBase::DEFAULT_INTERVAL, $blocking = false);

    public function perform(ResqueJobBase $job);
}