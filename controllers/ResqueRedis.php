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
            $this->backend = $backend;
            if (is_array($backend)) {
                $this->driver = new \Credis_Cluster($backend);
            } else {
                if (is_object($client)) {
                    $this->driver = $client;
                } else {
                    list($host, $port, $dsnDatabase, $user, $password, $options) = $this->backend->parseBackendDsn();
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
                    $args[0][$i] = $this->backend->namespaceRedis . $v;
                }
            } else {
                $args[0] = $this->backend->namespaceRedis . $args[0];
            }
        }
        try {
            return $this->driver->__call($name, $args);
        } catch (\CredisException $e) {
            throw new ResqueRedisException('Error communicating with Redis: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @return string
     */
    public function getPrefix()
    {
        return $this->backend->namespaceRedis;
    }

    /**
     * @param $string
     * @return string
     */
    public function removePrefix($string)
    {
        $prefix = $this->backend->namespaceRedis;

        if (substr($string, 0, strlen($prefix)) == $prefix) {
            $string = substr($string, strlen($prefix), strlen($string));
        }
        return $string;
    }
}
