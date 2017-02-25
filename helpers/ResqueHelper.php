<?php

namespace spiritdead\resque\helpers;

/**
 * Class ResqueHelper
 * @package spiritdead\resque\helpers
 */
class ResqueHelper
{
    /*
     * Generate an identifier to attach to a job for status tracking.
     * @return string
     */
    public static function generateJobId()
    {
        return md5(uniqid('', true));
    }
}