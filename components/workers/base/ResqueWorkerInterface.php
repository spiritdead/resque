<?php

namespace spiritdead\resque\components\workers\base;

use spiritdead\resque\components\jobs\base\ResqueJobBase;

/**
 * Interface ResqueWorkerInterface
 * @package spiritdead\resque\components\workers\base
 */
abstract class a {

}
interface ResqueWorkerInterface
{
    public static function all($resqueInst);

    public static function find($resqueInst, $workerId);

    public static function exists($resqueInst, $workerId);

    public function startup();

}