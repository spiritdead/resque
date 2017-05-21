<?php

namespace spiritdead\resque\components\jobs;

use spiritdead\resque\components\jobs\base\ResqueJobBase;
use spiritdead\resque\components\jobs\base\ResqueJobInterfaceBase;

/**
 * Interface ResqueJobInterface
 * @package spiritdead\resque\components\jobs
 */
interface ResqueJobInterface extends ResqueJobInterfaceBase
{
    public function setUp();

    public function tearDown();
}