<?php

namespace spiritdead\resque\helpers;

use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;

/**
 * Class ResqueLog
 * @package spiritdead\resque\helpers
 */
class ResqueLog extends AbstractLogger
{
    /**
     * @var bool
     */
    public $verbose;

    /**
     * @var bool
     */
    public $debug;

    /**
     * ResqueLog constructor.
     * @param bool $verbose
     * @param bool $debug
     */
    public function __construct($verbose = false, $debug = false)
    {
        $this->verbose = $verbose;
        $this->debug = $debug;
    }

    /**
     * Logs with an arbitrary level.
     *
     * @param mixed $level PSR-3 log level constant, or equivalent string
     * @param string $message Message to log, may contain a { placeholder }
     * @param array $context Variables to replace { placeholder }
     * @return null
     */
    public function log($level, $message, array $context = [])
    {
        if ($this->verbose) {
            fwrite(
                STDOUT,
                '[' . $level . '] [' . strftime('%T %Y-%m-%d') . '] ' . $this->interpolate($message, $context) . PHP_EOL
            );
            return;
        }

        if ($this->debug) {
            if (!($level === LogLevel::INFO || $level === LogLevel::DEBUG)) {
                fwrite(
                    STDOUT,
                    '[' . $level . '] ' . $this->interpolate($message, $context) . PHP_EOL
                );
            }
        }
    }

    /**
     * Fill placeholders with the provided context
     * @author Jordi Boggiano j.boggiano@seld.be
     *
     * @param  string $message Message to be logged
     * @param  array $context Array of variables to use in message
     * @return string
     */
    public function interpolate($message, array $context = [])
    {
        // build a replacement array with braces around the context keys
        $replace = [];
        foreach ($context as $key => $val) {
            $replace['{' . $key . '}'] = $val;
        }

        // interpolate replacement values into the message and return
        return strtr($message, $replace);
    }
}
