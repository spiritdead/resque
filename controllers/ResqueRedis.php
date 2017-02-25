<?php

namespace spiritdead\resque\controllers;

use spiritdead\resque\exceptions\ResqueRedisException;
use spiritdead\resque\models\ResqueBackend;

/**
 * Class Resque_Redis
 * @package spiritdead\resque\controllers
 */
class ResqueRedis
{
    /**
     * @var string
     */
    private $defaultNamespace = 'resque:';

    /**
     * @var null | ResqueBackend
     */
    public $backend = null;

    /**
     * @var \Credis_Client|null
     */
    public $driver = null;

    /**
     * @var array List of all commands in Redis that supply a key as their
     *    first argument. Used to prefix keys with the Resque namespace.
     */
    private $keyCommands = [
        'exists',
        'del',
        'type',
        'keys',
        'expire',
        'ttl',
        'move',
        'set',
        'setex',
        'get',
        'getset',
        'setnx',
        'incr',
        'incrby',
        'decr',
        'decrby',
        'rpush',
        'lpush',
        'llen',
        'lrange',
        'ltrim',
        'lindex',
        'lset',
        'lrem',
        'lpop',
        'blpop',
        'rpop',
        'sadd',
        'srem',
        'spop',
        'scard',
        'sismember',
        'smembers',
        'srandmember',
        'zadd',
        'zrem',
        'zrange',
        'zrevrange',
        'zrangebyscore',
        'zcard',
        'zscore',
        'zremrangebyscore',
        'sort',
        'rename',
        'rpoplpush'
    ];
    // sinterstore
    // sunion
    // sunionstore
    // sdiff
    // sdiffstore
    // sinter
    // smove
    // mget
    // msetnx
    // mset
    // renamenx

    /**
     * Set Redis namespace (prefix) default: resque
     * @param string $namespace
     */
    public function prefix($namespace)
    {
        if (substr($namespace, -1) !== ':' && $namespace != '') {
            $namespace .= ':';
        }
        $this->defaultNamespace = $namespace;
    }

    /**
     * Resque_Redis constructor.
     * @param ResqueBackend|ResqueBackend[] $backend
     * @param object|null $client
     * @throws ResqueRedisException
     */
    public function __construct(ResqueBackend $backend, $client = null)
    {
        try {
            if (is_array($backend)) {
                $this->driver = new \Credis_Cluster($backend);
            } else {
                if (is_object($client)) {
                    $this->driver = $client;
                } else {
                    list($host, $port, $dsnDatabase, $user, $password, $options) = $this->parseBackendDsn($backend);
                    // $user is not used, only $password

                    // Look for known Credis_Client options
                    $timeout = isset($options['timeout']) ? intval($options['timeout']) : null;
                    $persistent = isset($options['persistent']) ? $options['persistent'] : '';
                    $maxRetries = isset($options['max_connect_retries']) ? $options['max_connect_retries'] : 0;

                    $this->driver = new \Credis_Client($host, $port, $timeout, $persistent);
                    $this->driver->setMaxConnectRetries($maxRetries);
                    if ($password) {
                        $this->driver->auth($password);
                    }

                    // If we have found a database in our DSN, use it instead of the `$database`
                    // value passed into the constructor.
                    if ($dsnDatabase !== false) {
                        $this->backend->database = $dsnDatabase;
                    }
                }
            }

            if ($backend->database !== null) {
                $this->driver->select($backend->database);
            }
        } catch (\CredisException $e) {
            throw new ResqueRedisException('Error communicating with Redis: ' . $e->getMessage(), 0, $e);
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
     * @param ResqueBackend $backend
     * @return array An array of DSN compotnents, with 'false' values for any unknown components. e.g.
     *               [host, port, db, user, pass, options]
     */
    public function parseBackendDsn($backend)
    {
        $dsn = (string)$backend;

        if ($dsn == '') {
            // Use a sensible default for an empty DNS string
            $dsn = 'redis://' . ResqueBackend::DEFAULT_HOST;
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
        $port = isset($parts['port']) ? intval($parts['port']) : ResqueBackend::DEFAULT_HOST;

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
     * Magic method to handle all function requests and prefix key based
     * operations with the {self::$defaultNamespace} key prefix.
     *
     * @param string $name The name of the method called.
     * @param array $args Array of supplied arguments to the method.
     * @return mixed Return value from Resident::call() based on the command.
     * @throws ResqueRedisException
     */
    public function __call($name, $args)
    {
        if (in_array($name, $this->keyCommands)) {
            if (is_array($args[0])) {
                foreach ($args[0] AS $i => $v) {
                    $args[0][$i] = $this->defaultNamespace . $v;
                }
            } else {
                $args[0] = $this->defaultNamespace . $args[0];
            }
        }
        try {
            return $this->driver->__call($name, $args);
        } catch (\CredisException $e) {
            throw new ResqueRedisException('Error communicating with Redis: ' . $e->getMessage(), 0, $e);
        }
    }

    public function getPrefix()
    {
        return $this->defaultNamespace;
    }

    public function removePrefix($string)
    {
        $prefix = $this->defaultNamespace;

        if (substr($string, 0, strlen($prefix)) == $prefix) {
            $string = substr($string, strlen($prefix), strlen($string));
        }
        return $string;
    }
}
