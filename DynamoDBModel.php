<?php

namespace App\Services\DynamoDB;

use Aws\DynamoDb\DynamoDbClient;
use Exception;
use Illuminate\Database\Eloquent\Model;

class DynamoDBModel extends Model
{
    protected $KeySchema;
    protected $LocalSecondaryIndexes;
    protected $GlobalSecondaryIndexes;
    protected $AttributeDefinitions;
    protected $client;
    protected $marshaler;
    protected $_attributeFilter;

    private static $dynamoDb;


    /* ==============================================
     * 对model的特殊设置
     * =============================================*/

    /*创建db连接，如果为创建继承类对象，设置key*/
    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);

        $this->buildDynamoDBClient();

        if (!is_null($this->table)) {
            $this->setKey();
        }
    }

    /*通过本model创建1个全新的model*/
    public function getNewInstance($table, $attributes)
    {
        $model = $this->newFromBuilder($attributes);

        $model->setTable($table)
            ->setKey();

        return $model;
    }

    /*创建非继承类实例*/
    public function getSelfInstance($table, $attributes)
    {
        $this->setRawAttributes((array)$attributes, true);

        $this->exists = true;
        $this->guard([]);

        $this->setTable($table)
            ->setKey();

        return $this;
    }

    /*构建builder*/
    public function newQuery()
    {
        $builder = new DynamoDBBuilder();

        $builder->setModel($this);

        $builder->setClient($this->client);

        return $builder;
    }


    /* ==============================================
     * 需要重写的model功能,all/create/destory与父类一样，放在这里对比用
     * =============================================*/

    public static function all($columns = [])
    {
        $columns = is_array($columns) ? $columns : func_get_args();

        $instance = new static;

        return $instance->newQuery()->get($columns);
    }

    public function dall($columns = [])
    {
        return $this->newQuery()->get($columns);
    }

    public static function create(array $attributes = [])
    {
        $instance = new static($attributes);

        $instance->save();

        return $instance;
    }

    public function dcreate(array $attributes = [])
    {
        $this->forceFill($attributes);

        return $this->newQuery()->save();
    }

    public static function destroy($key)
    {
        $instance = new static;

        return $instance->newQuery()->destory($key);
    }

    public function ddestroy($key)
    {
        return $this->newQuery()->destory($key);
    }

    public function update(array $attributes = [])
    {
        return parent::update($attributes);
    }

    public function dupdate($key, array $attributes = [])
    {
        $this->forceFill($attributes);

        return $this->newQuery()->update($key);
    }

    public function save(array $options = [])
    {
        return $this->newQuery()->save();
    }


    /* ==============================================
     * 工具方法
     * =============================================*/

    //列出所有表名
    public function listTables()
    {
        return $this->client->listTables();
    }

    //描述表结构
    public function describeTable($table_name)
    {
        return $this->client->describeTable($table_name);
    }

    //判断表是否存在
    public function tableExists($table_name, $client)
    {
        return in_array($table_name, $client->listTables()['TableNames']);
    }

    //增加表
    public function addTable($define)
    {
        if (empty($define['TableName']) || empty($define['AttributeDefinitions']) || empty($define['KeySchema']) || empty($define['ProvisionedThroughput']['ReadCapacityUnits']) || empty($define['ProvisionedThroughput']['WriteCapacityUnits'])) {
            return false;
        }

        foreach ($define['KeySchema'] as $v) {
            $tmp[] = $v['AttributeName'];
        }

        foreach ($define['AttributeDefinitions'] as $k2 => $v2) {
            if (!in_array($v2['AttributeName'], $tmp)) {
                unset($define['AttributeDefinitions'][$k2]);
            }
        }
        $tablename_defination['AttributeDefinitions'] = array_values($define['AttributeDefinitions']);

        $tablename_defination['TableName'] = $define['TableName'];
        $tablename_defination['KeySchema'] = $define['KeySchema'];
        $tablename_defination['ProvisionedThroughput']['ReadCapacityUnits'] = $define['ProvisionedThroughput']['ReadCapacityUnits'];
        $tablename_defination['ProvisionedThroughput']['WriteCapacityUnits'] = $define['ProvisionedThroughput']['WriteCapacityUnits'];

        try {
            $this->client->createTable($tablename_defination);

            return true;
        } catch (Exception $e) {

            return false;
        }
    }

    //描述表
    public function descTable($tablename)
    {
        if (empty($tablename)) {
            return false;
        }
        try {
            return $this->client->describeTable(['TableName' => $tablename]);
        } catch (Exception $e) {
            return false;
        }
    }

    //修改表
    public function updateTable($define)
    {
        if (empty($define['TableName'])) {
            return false;
        }
        $tablename_defination['TableName'] = $define['TableName'];

        empty($define['AttributeDefinitions']) || ($tablename_defination['AttributeDefinitions'] = $define['AttributeDefinitions']);
        empty($define['KeySchema']) || ($tablename_defination['KeySchema'] = $define['KeySchema']);
        empty($define['ProvisionedThroughput']['ReadCapacityUnits']) || ($tablename_defination['ProvisionedThroughput']['ReadCapacityUnits'] = $define['ProvisionedThroughput']['ReadCapacityUnits']);
        empty($define['ProvisionedThroughput']['WriteCapacityUnits']) || ($tablename_defination['ProvisionedThroughput']['WriteCapacityUnits'] = $define['ProvisionedThroughput']['WriteCapacityUnits']);

        if (empty($tablename_defination)) {
            return false;
        }

        return $this->client->waitUntil('TableExists', $tablename_defination);
    }

    //删除表
    public function delTables($tablename)
    {
        try {
            $this->client->deleteTable(array(
                'TableName' => $tablename
            ));

            $this->client->waitUntil('TableNotExists', array(
                'TableName' => $tablename
            ));

            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    /* ==============================================
     * 有关dynamodb方面的特殊设置
     * =============================================*/

    /*创建dynamodb连接*/
    public function buildDynamoDBClient()
    {
        if (is_null(static::$dynamoDb)) {
            static::$dynamoDb = new DynamoDBClientBuilder();
            $this->client = static::$dynamoDb->getClient();
            $this->marshaler = static::$dynamoDb->getMarshal();
            $this->_attributeFilter = static::$dynamoDb->getAttributeFilter();
        }

        return static::$dynamoDb;
    }

    /*获取dynamodb句柄*/
    public function getClient()
    {
        return $this->client;
    }

    /*设置model所有索引属性和表结构属性*/
    public function setKey()
    {
        $table_desc = $this->describeTable(['TableName' => $this->table]);

        $this->primaryKey = '';
        $this->KeySchema = $table_desc['Table']['KeySchema'];
        $this->LocalSecondaryIndexes = isset($table_desc['Table']['LocalSecondaryIndexes']) ? $table_desc['Table']['LocalSecondaryIndexes'] : null;
        $this->GlobalSecondaryIndexes = isset($table_desc['Table']['GlobalSecondaryIndexes']) ? $table_desc['Table']['GlobalSecondaryIndexes'] : null;
        $this->AttributeDefinitions = collect($table_desc['Table']['AttributeDefinitions'])->keyBy('AttributeName')->all();

        return $this;
    }

    /* 获取主键，索引名为一维键，索引hash/range为二维键，属性名为值 */
    public function getKeySchema()
    {
        $rst = [];

        foreach ($this->KeySchema as $key) {
            $rst[$key['KeyType']] = $key['AttributeName'];
        }

        return $rst;
    }

    /*检查是否有本地二级索引*/
    public function hasLocalSecondaryIndexes()
    {
        return !empty($this->LocalSecondaryIndexes);
    }

    /* 索引名为一维键，索引hash/range为二维键，属性名为值 */
    public function getLocalSecondaryIndexes()
    {
        $rst = [];

        foreach ($this->LocalSecondaryIndexes as $l) {
            foreach ($l['KeySchema'] as $k) {
                $rst[$l['IndexName']][$k['KeyType']] = $k['AttributeName'];
            }
        }

        return $rst;
    }

    /*检查是否有全局二级索引*/
    public function hasGlobalSecondaryIndexes()
    {
        return !empty($this->GlobalSecondaryIndexes);
    }

    /* 获取全局二级索引，索引名为一维键，索引hash/range为二维键，属性名为值 */
    public function getGlobalSecondaryIndexes()
    {
        $rst = [];

        foreach ($this->GlobalSecondaryIndexes as $l) {
            foreach ($l['KeySchema'] as $k) {
                $rst[$l['IndexName']][$k['KeyType']] = $k['AttributeName'];
            }
        }

        return $rst;
    }

    /*获取所有的分区键*/
    public function getHash()
    {
        $hash = [];

        if (!is_null($this->GlobalSecondaryIndexes)) {
            foreach ($this->GlobalSecondaryIndexes as $l) {
                foreach ($l['KeySchema'] as $k) {
                    if ($k['KeyType'] == 'HASH') {
                        $hash[] = $k['AttributeName'];
                    }
                }
            }
        }

        foreach ($this->KeySchema as $key) {
            if ($key['KeyType'] == 'HASH') {
                $hash[] = $key['AttributeName'];
            }
        }

        return $hash;
    }

    /*对数字分区键的特殊处理*/
    public function translateType($item)
    {
        $_key = array_intersect_key($this->AttributeDefinitions, $item);

        foreach ($_key as $_k => $_v) {
            if ($_v['AttributeType'] == 'N') {
                $item[$_k] = (int)($item[$_k]);
            }
        }

        return $item;
    }

    /* ==============================================
     * 辅助方法
     * =============================================*/

    /*类型转换php->dynamodb*/
    public function marshalItem($item)
    {
        return $this->marshaler->marshalItem($item);
    }

    /*类型转换dynamodb->php*/
    public function unmarshalItem($item)
    {
        return $this->marshaler->unmarshalItem($item);
    }

    public function getAttributeFilter()
    {
        return $this->_attributeFilter;
    }

    /* ==============================================
     * 数据迁移
     * =============================================*/

    /*导出数据*/
    public function DynamoDBEmigration($tables = [], $limit = 40)
    {
        set_time_limit(0);

        $items = [];
        $tablename_defination = [];

        $tables = empty($tables) ? $this->client->listTables()['TableNames'] : $tables;

        foreach ($tables as $k => $x) {
            $v = $this->client->describeTable(['TableName' => $x]);
            $tablename_defination[$x]['TableName'] = $v['Table']['TableName'];
            $tablename_defination[$x]['AttributeDefinitions'] = $v['Table']['AttributeDefinitions'];
            $tablename_defination[$x]['KeySchema'] = $v['Table']['KeySchema'];
            $tablename_defination[$x]['ProvisionedThroughput']['ReadCapacityUnits'] = $v['Table']['ProvisionedThroughput']['ReadCapacityUnits'];
            $tablename_defination[$x]['ProvisionedThroughput']['WriteCapacityUnits'] = $v['Table']['ProvisionedThroughput']['WriteCapacityUnits'];
        }

        foreach ($tables as $k => $v) {
            $request = [
                'TableName' => $v,
                'Limit' => $limit,
            ];

            $response = $this->client->scan($request);

            foreach ($response['Items'] as $k2 => $v2) {
                $items[$v][$k2]['TableName'] = $v;
                $items[$v][$k2]['Item'] = $v2;
            }
        }

        return [
            'table_defination' => serialize($tablename_defination),
            'table_items' => serialize($items),
        ];
    }

    /*导入数据*/
    public function DynamoDBImmigration($table_defination, $table_items)
    {
        set_time_limit(0);

        $config = [
            'credentials' => [
                'key' => config('services.dynamodb.local_key'),
                'secret' => config('services.dynamodb.local_secret'),
            ],
            'region' => config('services.dynamodb.local_region'),
            'version' => 'latest',
            'endpoint' => config('services.dynamodb.local_endpoint'),
        ];
        $client = new DynamoDbClient($config);

        foreach ($table_defination as $k => $v) {
            $tmp = [];
            foreach ($v['KeySchema'] as $v2) {
                $tmp[] = $v2['AttributeName'];
            }

            foreach ($v['AttributeDefinitions'] as $k3 => $v3) {
                if (!in_array($v3['AttributeName'], $tmp)) {
                    unset($table_defination[$k]['AttributeDefinitions'][$k3]);
                }
            }

            $table_defination[$k]['AttributeDefinitions'] = array_values($table_defination[$k]['AttributeDefinitions']);

            if ($this->tableExists($k, $client)) {
                $client->updateTable($table_defination[$k]);
            } else {
                $client->createTable($table_defination[$k]);
            }
        }

        //插入数据
        foreach ($table_items as $k2 => $v2) {
            foreach ($v2 as $v3) {
                $client->putItem($v3);
            }
        }
    }

    /*同步云端数据到本地*/
    public function Immigration($tables = [], $limit = 40)
    {
        $data = $this->DynamoDBEmigration($tables, $limit);
        $this->DynamoDBImmigration(unserialize($data['table_defination']), unserialize($data['table_items']));
    }
}