<?php

namespace spiritdead\resque\components\jobs\base;

/**
 * Interface ResqueJobInterface
 * @package spiritdead\resque\jobs\base
 */
interface ResqueJobInterface
{
    /**
     * @return bool
     */
    public function perform();
}