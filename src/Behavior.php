<?php
/**
 * @link https://github.com/zhuravljov/yii2-queue-monitor
 * @copyright Copyright (c) 2017 Roman Zhuravlev
 * @license http://opensource.org/licenses/BSD-3-Clause
 */

namespace zhuravljov\yii\queue\monitor;

use Yii;
use yii\base\InvalidConfigException;
use yii\queue\cli\WorkerEvent;
use yii\queue\ErrorEvent;
use yii\queue\ExecEvent;
use yii\queue\JobEvent;
use yii\queue\PushEvent;
use yii\queue\Queue;
use yii\queue\cli\Queue as CliQueue;
use zhuravljov\yii\queue\monitor\records\ExecRecord;
use zhuravljov\yii\queue\monitor\records\PushRecord;
use zhuravljov\yii\queue\monitor\records\WorkerRecord;

/**
 * Queue Monitor Behavior
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
class Behavior extends \yii\base\Behavior
{
    /**
     * @var Queue
     * @inheritdoc
     */
    public $owner;
    /**
     * @var bool
     */
    public $canTrackWorkers = false;
    /**
     * @var Env
     */
    protected $env;

    /**
     * @param Env $env
     * @param array $config
     */
    public function __construct(Env $env, $config = [])
    {
        $this->env = $env;
        parent::__construct($config);
    }

    /**
     * @inheritdoc
     */
    public function events()
    {
        $events = [
            Queue::EVENT_AFTER_PUSH => 'afterPush',
            Queue::EVENT_BEFORE_EXEC => 'beforeExec',
            Queue::EVENT_AFTER_EXEC => 'afterExec',
            Queue::EVENT_AFTER_ERROR => 'afterError',
        ];
        if ($this->canTrackWorkers) {
            $events += [
                CliQueue::EVENT_WORKER_START => 'workerStart',
                CliQueue::EVENT_WORKER_STOP => 'workerStop',
            ];
        }

        return $events;
    }

    /**
     * @param PushEvent $event
     */
    public function afterPush(PushEvent $event)
    {
        $push = new PushRecord();
        $push->sender_name = $this->getSenderName();
        $push->job_uid = $event->id;
        $push->setJob($event->job);
        $push->ttr = $event->ttr;
        $push->delay = $event->delay;
        $push->pushed_at = time();
        $push->save(false);
    }

    /**
     * @param ExecEvent $event
     */
    public function beforeExec(ExecEvent $event)
    {
        $push = $this->getPushRecord($event);
        if (!$push) {
            return;
        }
        if ($push->isStopped()) {
            // Rejects job execution in case is stopped
            $event->handled = true;
            return;
        }
        $this->env->db->transaction(function () use ($event, $push) {
            $worker = $this->getWorkerRecord();

            $exec = new ExecRecord();
            $exec->push_id = $push->id;
            if ($worker) {
                $exec->worker_id = $worker->id;
            }
            $exec->attempt = $event->attempt;
            $exec->reserved_at = time();
            $exec->save(false);

            $push->first_exec_id = $push->first_exec_id ?: $exec->id;
            $push->last_exec_id = $exec->id;
            $push->save(false);

            if ($worker) {
                $worker->last_exec_id = $exec->id;
                $worker->save(false);
            }
        });
    }

    /**
     * @param ExecEvent $event
     */
    public function afterExec(ExecEvent $event)
    {
        $push = $this->getPushRecord($event);
        if (!$push) {
            return;
        }
        if ($push->last_exec_id) {
            ExecRecord::updateAll([
                'done_at' => time(),
                'error' => null,
                'retry' => false,
            ], [
                'id' => $push->last_exec_id
            ]);
        }
    }

    /**
     * @param ErrorEvent $event
     */
    public function afterError(ErrorEvent $event)
    {
        $push = $this->getPushRecord($event);
        if (!$push) {
            return;
        }
        if ($push->isStopped()) {
            // Breaks retry in case is stopped
            $event->retry = false;
        }
        if ($push->last_exec_id) {
            ExecRecord::updateAll([
                'done_at' => time(),
                'error' => $event->error,
                'retry' => $event->retry,
            ], [
                'id' => $push->last_exec_id
            ]);
        }
    }

    /**
     * @param WorkerEvent $event
     */
    public function workerStart(WorkerEvent $event)
    {
        $worker = new WorkerRecord();
        $worker->sender_name = $this->getSenderName();
        $worker->pid = $this->owner->getWorkerPid();
        $worker->started_at = time();
        $worker->save(false);
    }

    /**
     * @param WorkerEvent $event
     */
    public function workerStop(WorkerEvent $event)
    {
        $this->env->db->close(); // To reopen a lost connection
        if ($worker = $this->getWorkerRecord()) {
            $worker->finished_at = time();
            $worker->save(false);
        }
    }

    /**
     * @return string
     * @throws
     */
    protected function getSenderName()
    {
        foreach (Yii::$app->getComponents(false) as $id => $component) {
            if ($component === $this->owner) {
                return $id;
            }
        }
        throw new InvalidConfigException('Queue must be an application component.');
    }

    /**
     * @param JobEvent $event
     * @return PushRecord
     */
    protected function getPushRecord(JobEvent $event)
    {
        if ($event->id !== null) {
            return $this->env->db->useMaster(function () use ($event) {
                return PushRecord::find()
                    ->byJob($this->getSenderName(), $event->id)
                    ->one();
            });
        } else {
            return null;
        }
    }

    /**
     * @return null|WorkerRecord
     */
    protected function getWorkerRecord()
    {
        if (!$this->canTrackWorkers || !$this->owner->getWorkerPid()) {
            return null;
        }

        return $this->env->db->useMaster(function () {
            return WorkerRecord::find()
                ->byPid($this->owner->getWorkerPid())
                ->active()
                ->one();
        });
    }
}