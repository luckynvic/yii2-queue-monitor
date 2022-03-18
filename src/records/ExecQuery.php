<?php
/**
 * @link https://github.com/zhuravljov/yii2-queue-monitor
 * @copyright Copyright (c) 2017 Roman Zhuravlev
 * @license http://opensource.org/licenses/BSD-3-Clause
 */

namespace zhuravljov\yii\queue\monitor\records;

use yii\mongodb\ActiveQuery;

/**
 * Exec Query
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
class ExecQuery extends ActiveQuery
{
    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
    }

    /**
     * @inheritdoc
     * @return ExecRecord[]|array
     */
    public function all($db = null)
    {
        return parent::all($db);
    }

    /**
     * @inheritdoc
     * @return ExecRecord|array|null
     */
    public function one($db = null)
    {
        return parent::one($db);
    }
}
