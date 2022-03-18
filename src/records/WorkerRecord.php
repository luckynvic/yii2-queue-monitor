<?php
/**
 * @link https://github.com/zhuravljov/yii2-queue-monitor
 * @copyright Copyright (c) 2017 Roman Zhuravlev
 * @license http://opensource.org/licenses/BSD-3-Clause
 */

namespace zhuravljov\yii\queue\monitor\records;

use Yii;
use yii\mongodb\ActiveRecord;
use yii\helpers\ArrayHelper;
use zhuravljov\yii\queue\monitor\Env;
use zhuravljov\yii\queue\monitor\Module;

/**
 * Worker Record
 *
 * @property int $id
 * @property string $sender_name
 * @property string $host
 * @property int $pid
 * @property int $started_at
 * @property int $pinged_at
 * @property null|int $stopped_at
 * @property null|int $finished_at
 * @property null|int $last_exec_id
 *
 * @property null|ExecRecord $lastExec
 * @property ExecRecord[] $execs
 * @property array $execTotal
 *
 * @property string $status
 * @property int $execTotalStarted
 * @property int $execTotalDone
 * @property int $duration
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
class WorkerRecord extends ActiveRecord
{
    /**
     * @inheritdoc
     * @return WorkerQuery|object the active query used by this AR class.
     */
    public static function find()
    {
        return Yii::createObject(WorkerQuery::class, [get_called_class()]);
    }

    /**
     * @inheritdoc
     */
    public static function getDb()
    {
        return Env::ensure()->db;
    }

    /**
     * @inheritdoc
     */
    public static function collectionName()
    {
        return Env::ensure()->workerTableName;
    }

    /**
     * @return array list of attribute names.
     */
    public function attributes()
    {
        return [
            '_id',
            'sender_name',
            'host',
            'pid',
            'started_at',
            'pinged_at',
            'stopped_at',
            'finished_at',
            'last_exec_id',
        ];
    }

    public function getId()
    {
        return (string) $this->_id;
    }

    public function attributeLabels()
    {
        return [
            'id' => Module::t('main', 'ID'),
            'sender_name' => Module::t('main', 'Sender'),
            'host' => Module::t('main', 'Host'),
            'pid' => Module::t('main', 'PID'),
            'status' => Module::t('main', 'Status'),
            'started_at' => Module::t('main', 'Started At'),
            'execTotalStarted' => Module::t('main', 'Total Started'),
            'execTotalDone' => Module::t('main', 'Total Done'),
        ];
    }

    /**
     * @return ExecQuery|\yii\db\ActiveQuery
     */
    public function getLastExec()
    {
        return $this->hasOne(ExecRecord::class, ['_id' => 'last_exec_id']);
    }

    /**
     * @return ExecQuery|\yii\db\ActiveQuery
     */
    public function getExecs()
    {
        return $this->hasMany(ExecRecord::class, ['worker_id' => '_id']);
    }

    /**
     * @return ExecQuery|\yii\db\ActiveQuery
     */
    public function getExecTotal()
    {
        $started = ExecRecord::find()->where(['worker_id' => $this->id])->count();
        $done = ExecRecord::find()->where(['and', ['worker_id' => $this->id],['not', 'finished_at', null]])->count();
        return [
            'worker_id' => $this->id,
            'started' => $started,
            'done' => $done,
        ];
    }

    /**
     * @return int
     */
    public function getExecTotalStarted()
    {
        return ArrayHelper::getValue($this->execTotal, 'started', 0);
    }

    /**
     * @return int
     */
    public function getExecTotalDone()
    {
        return ArrayHelper::getValue($this->execTotal, 'done', 0);
    }

    /**
     * @return int
     */
    public function getDuration()
    {
        if ($this->finished_at) {
            return $this->finished_at - $this->started_at;
        }
        return time() - $this->started_at;
    }

    /**
     * @return string
     */
    public function getStatus()
    {
        $format = Module::getInstance()->formatter;
        if (!$this->lastExec) {
            return Module::t('main', 'Idle since {time}.', [
                'time' => $format->asRelativeTime($this->started_at),
            ]);
        }
        if ($this->lastExec->finished_at) {
            return Module::t('main', 'Idle after a job since {time}.', [
                'time' => $format->asRelativeTime($this->lastExec->finished_at),
            ]);
        }
        return Module::t('main', 'Busy since {time}.', [
            'time' => $format->asRelativeTime($this->lastExec->started_at),
        ]);
    }

    /**
     * @return bool
     */
    public function isIdle()
    {
        return !$this->lastExec || $this->lastExec->finished_at;
    }

    /**
     * @return bool marked as stopped
     */
    public function isStopped()
    {
        return !!$this->stopped_at;
    }

    /**
     * Marks as stopped
     */
    public function stop()
    {
        $this->stopped_at = time();
        $this->save(false);
    }
}
