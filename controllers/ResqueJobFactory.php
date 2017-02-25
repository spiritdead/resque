<?php

namespace spiritdead\resque\controllers;

use spiritdead\resque\exceptions\base\ResqueException;
use spiritdead\resque\jobs\base\ResqueJobInterface;

class ResqueJobFactory
{
    /**
     * @param $className
     * @param array $args
     * @param $queue
     * @return ResqueJobInterface
     * @throws ResqueException
     */
    public function create($className, $args, $queue)
    {
        if (!class_exists($className)) {
            throw new ResqueException(
                'Could not find job class ' . $className . '.'
            );
        }

        if (!method_exists($className, 'perform')) {
            throw new ResqueException(
                'Job class ' . $className . ' does not contain a perform method.'
            );
        }

        $instance = new $className;
        $instance->args = $args;
        $instance->queue = $queue;
        return $instance;
    }
}