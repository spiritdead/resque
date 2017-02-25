<?php
namespace spiritdead\resque\controllers\base;

use spiritdead\resque\jobs\base\ResqueJobInterface;

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