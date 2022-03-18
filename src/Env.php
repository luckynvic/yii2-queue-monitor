<?php
/**
 * @link https://github.com/zhuravljov/yii2-queue-monitor
 * @copyright Copyright (c) 2017 Roman Zhuravlev
 * @license http://opensource.org/licenses/BSD-3-Clause
 */

namespace zhuravljov\yii\queue\monitor;

use Yii;
use yii\base\BaseObject;
use yii\caching\Cache;
use yii\mongodb\Connection;
use yii\di\Instance;

/**
 * Class Env
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
class Env extends BaseObject
{
    /**
     * @var Cache|array|string
     */
    public $cache = 'cache';
    /**
     * @var Connection|array|string
     */
    public $db = 'mongodb';
    /**
     * @var string
     */
    public $pushTableName = 'queue_push';
    /**
     * @var string
     */
    public $execTableName = 'queue_exec';
    /**
     * @var string
     */
    public $workerTableName = 'queue_worker';
    /**
     * @var int
     */
    public $workerPingInterval = 15;

    /**
     * @return static
     */
    public static function ensure()
    {
        return Yii::$container->get(static::class);
    }

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $this->cache = Instance::ensure($this->cache, Cache::class);
        $this->db = Instance::ensure($this->db, Connection::class);
    }

    /**
     * @return bool
     */
    public function canListenWorkerLoop()
    {
        return !!$this->workerPingInterval;
    }

    /**
     * @return string
     */
    public function getHost()
    {
        return $_SERVER['HOSTNAME'] ?? '127.0.0.1';
    }
}
