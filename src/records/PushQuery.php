<?php
/**
 * @link https://github.com/zhuravljov/yii2-queue-monitor
 * @copyright Copyright (c) 2017 Roman Zhuravlev
 * @license http://opensource.org/licenses/BSD-3-Clause
 */

namespace zhuravljov\yii\queue\monitor\records;

use DateInterval;
use DateTime;
use yii\mongodb\ActiveQuery;
use yii\mongodb\Query;

/**
 * Push Query
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
class PushQuery extends ActiveQuery
{
    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
    }

    /**
     * @param int $id
     * @return $this
     */
    public function byId($id)
    {
        return $this->andWhere(['_id' => $id]);
    }

    /**
     * @param string $senderName
     * @param string $jobUid
     * @return $this
     */
    public function byJob($senderName, $jobUid)
    {
        return $this
            ->andWhere(['sender_name' => $senderName])
            ->andWhere(['job_uid' => $jobUid])
            ->orderBy(['_id' => SORT_DESC])
            ->limit(1);
    }

    /**
     * @return $this
     */
    public function waiting()
    {
        return $this->andWhere(['or', ['last_exec_id' => null], ['last_exec_retry' => true]])
            ->andWhere(['stopped_at' => null]);
    }

    /**
     * @return $this
     */
    public function inProgress()
    {
        return $this
            ->andWhere(['not', 'last_exec_id', null])
            ->andWhere(['last_exec_finished_at' => null]);
    }

    /**
     * @return $this
     */
    public function done()
    {
        return $this
            ->andWhere(['not', 'last_exec_finished_at', null])
            ->andWhere(['last_exec_retry' => false]);
    }

    /**
     * @return $this
     */
    public function success()
    {
        return $this
            ->done()
            ->andWhere(['last_exec_error' => null]);
    }

    /**
     * @return $this
     */
    public function buried()
    {
        return $this
            ->done()
            ->andWhere(['not', 'last_exec_error', null]);
    }

    /**
     * @return $this
     */
    public function hasFails()
    {
        return $this
            ->andWhere(['exists', new Query([
                'from' => ['exec' => ExecRecord::tableName()],
                'where' => '{{exec}}.[[push_id]] = {{push}}.[[id]] AND {{exec}}.[[error]] IS NOT NULL',
            ])]);
    }

    /**
     * @return $this
     */
    public function stopped()
    {
        return $this->andWhere(['not', 'stopped_at', null]);
    }

    /**
     * @param string $interval
     * @link https://www.php.net/manual/en/dateinterval.construct.php
     * @return $this
     */
    public function deprecated($interval)
    {
        $min = (new DateTime())->sub(new DateInterval($interval))->getTimestamp();
        return $this->andWhere(['<', 'pushed_at', $min]);
    }

    /**
     * @inheritdoc
     * @return PushRecord[]|array
     */
    public function all($db = null)
    {
        return parent::all($db);
    }

    /**
     * @inheritdoc
     * @return PushRecord|array|null
     */
    public function one($db = null)
    {
        return parent::one($db);
    }
}
