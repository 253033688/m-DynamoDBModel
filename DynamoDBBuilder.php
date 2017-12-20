<?php

namespace App\Services\DynamoDB;

use Illuminate\Support\Collection;

class DynamoDBBuilder
{
    protected $model;
    protected $client;
    protected $_andWhere = [];
    protected $_orWhere = [];
    protected $_orAndWhere = [];
    protected $_andOrWhere = [];
    protected $_page_array;
    protected $_deleteKey;
    protected $ExclusiveStartKey;
    protected $isFirstPage;
    protected $isLastPage;


    /* ==============================================
     * 对外提供的查询接口
     * =============================================*/

    public function find(array $key, array $columns = [])
    {
        $column = $key[0];
        isset($key[1]) ? ($operator = $key[1]) : ($operator = null);
        isset($key[2]) ? ($value = $key[2]) : ($value = null);

        $data = $this->where($column, $operator, $value)->get($columns);
        return $data->isEmpty() ? collect([]) : collect($data->first());
    }

    public function first($columns = [])
    {
        $item = $this->get($columns, 1);

        return $item->first();
    }

    public function all($columns = [])
    {
        return $this->get($columns);
    }

    public function count()
    {
        return $this->get()->count();
    }


    /* ==============================================
     * 构建where条件
     * =============================================*/

    public function where($column, $operator = null, $value = null)
    {
        //$column为数组情况
        if (is_array($column)) {
            foreach ($column as $key => $value) {
                $this->where($key, '=', $value);
            }

            return $this;
        }

        if (func_num_args() === 2 || is_null($value)) {
            return $this->where($column, '=', $operator);
        }

        //闭包情况
        if ($column instanceof \Closure) {
            $newBuilder = new static();

            call_user_func($column, $newBuilder);

            $this->_andOrWhere = array_merge($this->_andOrWhere, $newBuilder->getOrWhere());
        } else {
            $this->_andWhere[] = [
                'key' => $column,
                'operator' => $operator,
                'value' => $value,
            ];
        }

        return $this;
    }

    public function andWhere($column, $operator = null, $value = null)
    {
        return $this->where($column, $operator, $value);
    }

    public function orWhere($column, $operator = null, $value = null)
    {
        if (is_array($column)) {
            foreach ($column as $key => $value) {
                return $this->orWhere($key, '=', $value);
            }
        }

        if (func_num_args() === 2) {
            return $this->orWhere($column, '=', $operator);
        }

        //闭包情况
        if ($column instanceof \Closure) {
            $newBuilder = new static();

            call_user_func($column, $newBuilder);

            $this->_orAndWhere = array_merge($this->_orAndWhere, $newBuilder->getAndWhere());
        } else {
            $this->_orWhere[] = [
                'key' => $column,
                'operator' => $operator,
                'value' => $value,
            ];
        }

        return $this;
    }

    protected function getAndWhere()
    {
        return $this->_andWhere;
    }

    protected function getOrWhere()
    {
        return $this->_orWhere;
    }


    /* ==============================================
     * 基础查询方法
     * =============================================*/

    /*不带分页的查询组装*/
    public function get($columns = [])
    {
        $res = collect([]);
//dd($this->_andWhere, $this->_orAndWhere, $this->_orWhere, $this->_andOrWhere);
        if (empty($this->_andWhere) && empty($this->_orAndWhere) && empty($this->_orWhere) && empty($this->_andOrWhere)) {
            return $this->_query([], $columns);
        }

        if (!empty($this->_andOrWhere)) {
            foreach ($this->_andOrWhere as $_av) {
                $res = $res->merge($this->_query(array_merge($this->_andWhere, [$_av]), $columns));
            }
        } else {
            !empty($this->_andWhere) && ($res = $res->merge($this->_query($this->_andWhere, $columns)));
        }

        !empty($this->_orAndWhere) && ($res = $res->merge($this->_query($this->_orAndWhere, $columns)));

        foreach ($this->_orWhere as $w) {
            $res = $res->merge($this->_query([$w], $columns));
        }

        return $res->unique();
    }

    private function _query($where, $columns = [])
    {
        $query = [];

        if ($this->conditionsContainKey($where)) {
            $query['index'] = $this->conditionsContainKey($where);
        }

        $query += $this->buildQuery($query, $where);

        $query['TableName'] = $this->model->getTable();

        if (!empty($columns)) {
            //将主键放入投影之中，保证无论如何后返回的值中都有主键
            $columns += array_values($this->model->getKeySchema());

            $query['ProjectionExpression'] = implode(',', $columns);
        }

        key_exists('index', $query) ?
            ($iterator = $this->client->query($query)['Items']) :
            ($iterator = $this->client->scan($query)['Items']);

        $results = [];

        foreach ($iterator as $item) {
            $item = $this->model->unmarshalItem($item);

            $model = $this->model->getSelfInstance($this->model->getTable(), $item);

            //单独获取主键
            $_primaryKey = $model[$model->getKeySchema()['HASH']];
            isset($model->getKeySchema()['RANGE']) && ((string)$_primaryKey .= (string)($model[$model->getKeySchema()['RANGE']]));

            $results[$_primaryKey] = $model->toArray();
        }

        return new Collection($results);
    }

    /*带分页的scan*/
    public function scanForPage($requests, $columns = [], $limit = 10)
    {
        return $this->_scan($requests, $this->_andWhere, $columns, $limit);
    }

    public function _scan($requests, $where, $columns = [], $limit = 10)
    {
        $scan = [];
//        dd($this->_andWhere, $this->_orAndWhere, $this->_orWhere, $this->_andOrWhere);
        //分页功能部分
        $page = $requests->get('page');
        $this->isFirstPage = $this->isLastPage = false;
        $this->_page_array = $requests->session()->get('pageArray');

        //重置首页
        if (empty($this->_page_array) || empty($page)) {
            $this->isFirstPage = true;
            $this->_page_array = [];
        }

        //上一页
        if ($page === 'pre') {
            //上一页是第一页情况
            if (count($this->_page_array) < 3) {
                $this->isFirstPage = true;
                $this->_page_array = [];
            }

            array_pop($this->_page_array);
            array_pop($this->_page_array);
            if (($len = count($this->_page_array)) > 0) {
                $this->ExclusiveStartKey = $this->_page_array[$len - 1];
            }
        }

        //下一页
        if ($page === 'next' && ($len = count($this->_page_array)) > 0) {
            $this->ExclusiveStartKey = $this->_page_array[$len - 1];
        }

        if (!empty($this->ExclusiveStartKey)) {
            $scan['ExclusiveStartKey'] = $this->ExclusiveStartKey;
        }

        //查询部分
        $scan += $this->buildQuery([], $where);

        $scan['TableName'] = $this->model->getTable();

        if (!empty($columns)) {
            //将主键放入投影之中，保证无论如何后返回的值中都有主键
            $columns += array_values($this->model->getKeySchema());

            $query['ProjectionExpression'] = implode(',', $columns);
        }

        if ($limit !== -1) {
            $scan['Limit'] = intval($limit);
        }
//dd($scan);
        $res = $this->client->scan($scan);

        if (empty($this->_page_array) || !empty($this->_page_array) && !is_null($this->_page_array[count($this->_page_array) - 1])) {
            array_push($this->_page_array, array_get($res, 'LastEvaluatedKey'));
        }
        $requests->session()->put('pageArray', $this->_page_array);

        if (is_null(array_get($res, 'LastEvaluatedKey'))) {
            $this->isLastPage = true;
        } else {
            $this->isLastPage = false;
        }

        $results = [];
        $iterator = $res['Items'];

        foreach ($iterator as $item) {
            $item = $this->model->unmarshalItem($item);

            $model = $this->model->getSelfInstance($this->model->getTable(), $item);

            //单独获取主键
            $_primaryKey = $model[$model->getKeySchema()['HASH']];
            isset($model->getKeySchema()['RANGE']) && ((string)$_primaryKey .= (string)($model[$model->getKeySchema()['RANGE']]));

            $results[$_primaryKey] = $model->toArray();
        }

        $data = new Collection($results);

        return [
            'data' => $data,
            'page' => [
                'isFirstPage' => $this->isFirstPage,
                'isLastPage' => $this->isLastPage
            ]
        ];
    }

    /* ==============================================
     * 查询基础方法
     * =============================================*/

    /**
     * 获取跟where条件相匹配的主键或二级索引，按主键、本地二级索引、全局二级索引顺序返回
     * 如有返回值，key标识是那种键
     *
     * @return array|bool
     */
    protected function conditionsContainKey($where = [])
    {
        if (empty($where)) {
            return false;
        }

        $model = $this->model;

        $conditionKeys = array_pluck($where, 'key');

        //判断主键
        $KeySchema = $model->getKeySchema();

        if ($this->_checkKeyOperation($KeySchema['HASH'], $where)) {

            //没有排序键或where条件包括排序键情况
            if (!isset($KeySchema['RANGE']) || isset($KeySchema['RANGE']) && in_array($KeySchema['RANGE'], $conditionKeys)) {
                return ['primaryKey' => $KeySchema];
            }

            //判断本地二级索引，因为和主键的分区键一样，所用在此次判断
            if ($model->hasLocalSecondaryIndexes()) {
                $LocalSecondaryIndexes = $model->getLocalSecondaryIndexes();

                foreach ($LocalSecondaryIndexes as $li => $lv) {
                    if (in_array($lv['RANGE'], $conditionKeys)) {
                        //只要有一个条件满足即返回，不再考虑以后的了
                        return ['LocalSecondaryIndexes' => [$li => $lv]];
                    }
                }
            }

            return ['primaryKey' => ['HASH' => $KeySchema['HASH']]];
        }

        //判断全局二级索引
        if ($model->hasGlobalSecondaryIndexes()) {
            $GlobalSecondaryIndexes = $model->getGlobalSecondaryIndexes();

            foreach ($GlobalSecondaryIndexes as $gi => $gv) {
                if ($this->_checkKeyOperation($gv['HASH'], $where)) {

                    if (!isset($gv['RANGE']) || isset($gv['RANGE']) && in_array($gv['RANGE'], $conditionKeys)) {
                        return ['GlobalSecondaryIndexes' => [$gi => $gv]];
                    }

                    return ['GlobalSecondaryIndexes' => [$gi => $gv['HASH']]];
                }
            }
        }

        return false;
    }

    /*
     * 判断拟作为key的where条件的operation是否部位=
     *
     * */
    private function _checkKeyOperation($key, $where)
    {
        foreach ($where as $_w) {
            if ($key === $_w['key'] && $_w['operator'] === '=') {
                return true;
            }
        }

        return false;
    }

    /**
     * 组装查询条件的如下元素：
     * 1.IndexName（如为二级索引）
     * 2.KeyConditionExpression
     * 3.ExpressionAttributeValues
     * 4.FilterExpression
     *
     * @param $condition
     * @return array
     */
    protected function buildQuery($condition, $where)
    {
        $KeyConditionExpression = $FilterExpression = '';
        $request = $ExpressionAttributeValues = $_where = $_fields = [];

        if (isset($condition['index'])) {
            //获取主键key=>value
            if (isset($condition['index']['primaryKey'])) {
                $_fields = $condition['index']['primaryKey'];
            }

            //获取本地二级索引key=>value
            if (isset($condition['index']['LocalSecondaryIndexes'])) {
                $request['IndexName'] = array_keys($condition['index']['LocalSecondaryIndexes'])[0];
                $_fields = $condition['index']['LocalSecondaryIndexes'][$request['IndexName']];
            }

            //获取全局二级索引key=>value
            if (isset($condition['index']['GlobalSecondaryIndexes'])) {
                $request['IndexName'] = array_keys($condition['index']['GlobalSecondaryIndexes'])[0];
                $_fields = $condition['index']['GlobalSecondaryIndexes'][$request['IndexName']];
            }
        }

        //where去重，fields替换为where中的值
        $_where = $this->_buildWhereAndFields($where, $_fields);

        //生成查询表达式
        foreach ($_fields as $k => $v) {
            $KeyConditionExpression .= empty($KeyConditionExpression) ? $this->_buildExpression($k, $v)[0] : ' and ' . $this->_buildExpression($k, $v)[0];
            $ExpressionAttributeValues = $ExpressionAttributeValues + $this->_buildExpression($k, $v)[1];
        }

        //生成过滤表达式
        if (!empty($_where)) {
            $FilterExpression = $this->_buildFilterExpression($_where, $ExpressionAttributeValues);
        }

        if (!empty($KeyConditionExpression)) {
            $request['KeyConditionExpression'] = $KeyConditionExpression;
        }
        if (!empty($ExpressionAttributeValues)) {
            $request['ExpressionAttributeValues'] = $ExpressionAttributeValues;
        }
        if (!empty($FilterExpression)) {
            $request['FilterExpression'] = $FilterExpression;
        }

        return $request;
    }

    /**
     * 1.将fields中的range、hash替换为key=>value
     * 2.将where中与fields中重复的去掉，但where的结构不变
     *
     * @param $where
     * @param $fields
     * @return mixed
     */
    protected function _buildWhereAndFields($where, &$fields)
    {
        foreach ($where as $_k => $_v) {
            if (in_array($_v['key'], $fields)) {

                //按对应字段where条件保存
                $fields[] = $_v;

                //去掉where中重复的
                unset($where[$_k]);
            }
        }

        if (isset($fields['HASH']) || isset($fields['RANGE'])) {
            unset($fields['HASH'], $fields['RANGE']);
        }

        return $where;
    }

    /**
     * 将where条件组装成$FilterExpression和$ExpressionAttributeValues
     *
     * @param $where
     * @param $ExpressionAttributeValues
     * @param $join
     * @return string
     */
    protected function _buildFilterExpression($where, &$ExpressionAttributeValues)
    {
        $_FilterExpression = '';

        foreach ($where as $_k => $_v) {
            $_FilterExpression .= empty($_FilterExpression) ? $this->_buildExpression($_k, $_v)[0] : ' and ' . $this->_buildExpression($_k, $_v)[0];
            $ExpressionAttributeValues = $ExpressionAttributeValues + $this->_buildExpression($_k, $_v)[1];

            unset($where[$_k]);
        }

        return $_FilterExpression;
    }

    /**
     * 生成占位符和按dynamodb规则对应的值
     * $k为规避相同key时的标识
     *
     * @param $condition
     * @return array|string
     */
    protected function _buildExpression($k, $condition)
    {
        $result = [];

        if ($condition['operator'] == 'BETWEEN') {
            $result[0] = $condition['key'] . ' BETWEEN :v_' . $condition['key'] . $k . 'min' . ' AND :v_' . $condition['key'] . $k . 'max';
            $result[1] = [
                $this->model->marshalItem([':v_' . $condition['key'] . $k . 'min' => $condition['value']['min']]),
                $this->model->marshalItem([':v_' . $condition['key'] . $k . 'max' => $condition['value']['max']]),
            ];

            return $result;
        }

        if ($condition['operator'] == 'IN') {
            $result[0] = $condition['key'] . ' IN (:v_' . $condition['key'] . $k . ')';
            $result[1] = $this->model->marshalItem([':v_' . $condition['key'] . $k => implode(',', $condition['value'])]);

            return $result;
        }

        if ($condition['operator'] == 'NOTIN') {
            $result[0] = '!' . $condition['key'] . ' IN (:v_' . $condition['key'] . $k . ')';
            $result[1] = $this->model->marshalItem([':v_' . $condition['key'] . $k => implode(',', $condition['value'])]);

            return $result;
        }

        if ($condition['operator'] == 'attribute_exists') {
            $result[0] = 'attribute_exists(' . $condition['key'] . ')';
            $result[1] = [];

            return $result;
        }

        if ($condition['operator'] == 'attribute_not_exists') {
            $result[0] = 'attribute_not_exists(' . $condition['key'] . ')';
            $result[1] = [];

            return $result;
        }

        if ($condition['operator'] == 'attribute_type') {
            $result[0] = 'attribute_type(' . $condition['key'] . ',:v_' . $condition['key'] . $k . ')';
            $result[1] = $this->model->marshalItem([':v_' . $condition['key'] . $k => $condition['value']]);

            return $result;
        }

        if ($condition['operator'] == 'BEGINS_WITH') {
            $result[0] = 'begins_with(' . $condition['key'] . ', :v_' . $condition['key'] . $k . ')';
            $result[1] = $this->model->marshalItem([':v_' . $condition['key'] . $k => $condition['value']]);

            return $result;
        }

        if ($condition['operator'] == 'contains') {
            $result[0] = 'contains(' . $condition['key'] . ',:v_' . $condition['key'] . $k . ')';
            $result[1] = $this->model->marshalItem([':v_' . $condition['key'] . $k => $condition['value']]);

            return $result;
        }

        if ($condition['operator'] == 'size') {
            $result[0] = 'size(' . $condition['key'] . ')' . $condition['value']['operator'] . ':v_' . $condition['key'] . $k;
            $result[1] = $this->model->marshalItem([':v_' . $condition['key'] . $k => $condition['value']['value']]);

            return $result;
        }

        if (in_array($condition['operator'], ['=', '>', '<', '<>', '>=', '<='])) {
            $result[0] = $condition['key'] . $condition['operator'] . ':v_' . $condition['key'] . $k;
            $result[1] = $this->model->marshalItem([':v_' . $condition['key'] . $k => $condition['value']]);

            return $result;
        }

        return false;
    }

    /* ==============================================
     * 增删改
     * =============================================*/

    public function destory($key)
    {
        $query = [
            'TableName' => $this->model->getTable(),
            'Key' => $this->model->marshalItem($key),
        ];

        $result = $this->client->deleteItem($query);
        $status = array_get($result->toArray(), '@metadata.statusCode');

        return $status == 200;
    }

    public function save()
    {
        $item = $this->model->getAttributes();
        $this->model->getAttributeFilter()->filter($item);

        try {
            $this->client->putItem([
                'TableName' => $this->model->getTable(),
                'Item' => $this->model->marshalItem($item),
            ]);

            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    public function update($key)
    {
        $item = $this->model->getAttributes();
        $this->model->getAttributeFilter()->filter($item);

        $UpdateExpression = 'set ';
        $ExpressionAttributeValues = [];

        $i = 0;
        foreach ($item as $k => $v) {
            $condition = [
                'key' => $k,
                'operator' => '=',
                'value' => $v,
            ];

            $UpdateExpression .= $this->_buildExpression($i, $condition)[0] . ', ';
            $ExpressionAttributeValues = $ExpressionAttributeValues + $this->_buildExpression($i, $condition)[1];

            $i++;
        }

        try {
            $this->client->updateItem([
                'TableName' => $this->model->getTable(),
                'Key' => $this->model->marshalItem($key),
                'UpdateExpression' => rtrim($UpdateExpression, ', '),
                'ExpressionAttributeValues' => $ExpressionAttributeValues,
            ]);

            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    /* ==============================================
     * 其他
     * =============================================*/

    public function getModel()
    {
        return $this->model;
    }

    public function setModel($model)
    {
        $this->model = $model;
    }

    public function getClient()
    {
        return $this->client;
    }

    public function setClient($client)
    {
        $this->client = $client;
    }
}