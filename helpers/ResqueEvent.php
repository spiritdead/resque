<?php

namespace spiritdead\resque\helpers;

/**
 * Class ResqueHelper
 * @package spiritdead\resque\helpers
 */
class ResqueEvent
{
    /**
     * @var array Array containing all registered callbacks, indexed by event name.
     */
    private $events = [];

    /**
     * Raise a given event with the supplied data.
     *
     * @param string $event Name of event to be raised.
     * @param mixed $data Optional, any data that should be passed to each callback.
     * @return true
     */
    public function trigger($event, $data = null)
    {
        if (!is_array($data)) {
            $data = [$data];
        }

        if (empty($this->events[$event])) {
            return true;
        }

        foreach ($this->events[$event] as $callback) {
            if (!is_callable($callback)) {
                continue;
            }
            call_user_func_array($callback, $data);
        }

        return true;
    }

    /**
     * Listen in on a given event to have a specified callback fired.
     *
     * @param string $event Name of event to listen on.
     * @param mixed $callback Any callback callable by call_user_func_array.
     * @return true
     */
    public function listen($event, $callback)
    {
        if (!isset($this->events[$event])) {
            $this->events[$event] = [];
        }

        $this->events[$event][] = $callback;
        return true;
    }

    /**
     * Stop a given callback from listening on a specific event.
     *
     * @param string $event Name of event.
     * @param mixed $callback The callback as defined when listen() was called.
     * @return true
     */
    public function stopListening($event, $callback)
    {
        if (!isset($this->events[$event])) {
            return true;
        }

        $key = array_search($callback, $this->events[$event]);
        if ($key !== false) {
            unset($this->events[$event][$key]);
        }

        return true;
    }

    /**
     * Call all registered listeners.
     */
    public function clearListeners()
    {
        $this->events = [];
    }
}

