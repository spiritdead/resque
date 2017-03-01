<?php

namespace spiritdead\resque\models;

/**
 * Class ResqueBackend
 * @package spiritdead\resque\models
 */
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
     * namespace base used in redis server
     */
    const DEFAULT_NAMESPACE_REDIS = 'resque:';

    const DEFAULT_NAMESPACE_WORKERS = '';

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
     * Namespace base used in redis server
     * @var string
     */
    public $namespaceRedis;

    /**
     * Namespace base used by the workers
     * @var string
     */
    public $namespaceWorkers;

    /**
     * ResqueBackend constructor.
     * @param string $server
     * @param string $port
     * @param int $database
     * @param string $namespace
     * @param string $namespaceWorkers
     */
    public function __construct(
        $server = self::DEFAULT_HOST,
        $port = self::DEFAULT_PORT,
        $database = self::DEFAULT_DATABASE,
        $namespace = self::DEFAULT_NAMESPACE_REDIS,
        $namespaceWorkers = self::DEFAULT_NAMESPACE_WORKERS
    ) {
        $this->server = $server;
        $this->port = $port;
        $this->database = $database;
        $this->namespaceRedis = $namespace;
        if(empty($namespaceWorkers)){
            $this->namespaceWorkers = php_uname('n');
        }
    }


    /**
     * Parse a DSN string, which can have one of the following formats:
     *
     * - host:port
     * - redis://user:pass@host:port/db?option1=val1&option2=val2
     * - tcp://user:pass@host:port/db?option1=val1&option2=val2
     * - unix:///path/to/redis.sock
     *
     * Note: the 'user' part of the DSN is not used.
     *
     * @return array An array of DSN compotnents, with 'false' values for any unknown components. e.g.
     *               [host, port, db, user, pass, options]
     */
    public function parseBackendDsn()
    {
        $dsn = (string)$this;

        if ($dsn == '') {
            // Use a sensible default for an empty DNS string
            $dsn = 'redis://' . self::DEFAULT_HOST;
        }
        if (substr($dsn, 0, 7) === 'unix://') {
            return [
                $dsn,
                null,
                false,
                null,
                null,
                null
            ];
        }
        $parts = parse_url($dsn);

        // Check the URI scheme
        $validSchemes = ['redis', 'tcp'];
        if (isset($parts['scheme']) && !in_array($parts['scheme'], $validSchemes)) {
            throw new \InvalidArgumentException("Invalid DSN. Supported schemes are " . implode(', ', $validSchemes));
        }

        // Allow simple 'hostname' format, which `parse_url` treats as a path, not host.
        if (!isset($parts['host']) && isset($parts['path'])) {
            $parts['host'] = $parts['path'];
            unset($parts['path']);
        }

        // Extract the port number as an integer
        $port = isset($parts['port']) ? intval($parts['port']) : self::DEFAULT_HOST;

        // Get the database from the 'path' part of the URI
        $database = false;
        if (isset($parts['path'])) {
            // Strip non-digit chars from path
            $database = intval(preg_replace('/[^0-9]/', '', $parts['path']));
        }

        // Extract any 'user' and 'pass' values
        $user = isset($parts['user']) ? $parts['user'] : false;
        $pass = isset($parts['pass']) ? $parts['pass'] : false;

        // Convert the query string into an associative array
        $options = [];
        if (isset($parts['query'])) {
            // Parse the query string into an array
            parse_str($parts['query'], $options);
        }

        return [
            $parts['host'],
            $port,
            $database,
            $user,
            $pass,
            $options
        ];
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return sprintf('%s:%s', $this->server, $this->port);
    }
}