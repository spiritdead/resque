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

    public static function all($resqueInst);

    public static function find($resqueInst, $workerId);

    public static function exists($resqueInst, $workerId);

    public function registerWorker();

    public function unregisterWorker();

    public function workingOn($job);

    public function doneWorking();

    public function getWorking();

    public function pruneDeadWorkers();
}