<?php

namespace spiritdead\resque\models;

class ResqueBackend
{
    /**
     * A default host to connect to
     */
    const DEFAULT_HOST = 'localhost';

    /**
     * The default Redis port
     */
    const DEFAULT_PORT = '6379';

    /**
     * The default Redis Database number
     */
    const DEFAULT_DATABASE = 0;

    /**
     * @var string
     */
    public $server;

    /**
     * @var int
     */
    public $port;

    /**
     * @var int
     */
    public $database;

    /**
     * ResqueBackend constructor.
     * @param string $server
     * @param int $port
     * @param int $database
     */
    public function __construct(
        $server = self::DEFAULT_HOST,
        $port = self::DEFAULT_PORT,
        $database = self::DEFAULT_DATABASE
    ) {
        $this->server = $server;
        $this->port = $port;
        $this->database = $database;
    }

    public function __toString()
    {
        return sprintf('%s:%s', $this->server, $this->port);
    }
}