<?php
namespace spiritdead\resque\controllers\base;

use spiritdead\resque\components\jobs\ResqueJobInterface;

/**
 * Interface ResqueJobFactoryInterface
 * @package spiritdead\resque\controllers\base
 */
interface ResqueJobFactoryInterface
{
    /**
     * @param $className
     * @param array $args
     * @param $queue
     * @return ResqueJobInterface
     */
    public function create($className, $args, $queue);
}