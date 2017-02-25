<?php

namespace spiritdead\resque\components\jobs\base;

/**
 * Interface ResqueJobInterface
 * @package spiritdead\resque\jobs\base
 */
interface ResqueJobInterfaceBase
{
    /**
     * @return bool
     */
    public function perform();
}