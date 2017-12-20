<?php

namespace App\Services\DynamoDB\tests;

use App\Services\DynamoDB\DynamoDBModel;

class DynamoDbTest extends TestCase
{
    private static $model = null;

    public function __construct($name = null, array $data = array(), $dataName = '')
    {
        parent::__construct($name, $data, $dataName);

        if (self::$model === null) {
            self::$model = new DynamoDBModel();
        }
    }

    public function test()
    {
        $this->assertEquals(-1, -1);
    }

    //注：本地数据库无法建立二级索引
    public function testCreateTable()
    {
        $tableDefinition = [
            'TableName' => 'testDB',
            'AttributeDefinitions' => [
                ['AttributeName' => 'id1', 'AttributeType' => 'N'],
                ['AttributeName' => 'id2', 'AttributeType' => 'S'],
                ['AttributeName' => 'id3', 'AttributeType' => 'N'],
                ['AttributeName' => 'id4', 'AttributeType' => 'S'],
                ['AttributeName' => 'id5', 'AttributeType' => 'N'],
                ['AttributeName' => 'id6', 'AttributeType' => 'S']
            ],
            'KeySchema' => [
                ['AttributeName' => 'id1', 'KeyType' => 'HASH'],
                ['AttributeName' => 'id2', 'KeyType' => 'RANGE'],
            ],
            'GlobalSecondaryIndexes' => [
                [
                    'IndexName' => 'Index1',
                    'KeySchema' => [
                        ['AttributeName' => 'id3', 'KeyType' => 'HASH'],
                        ['AttributeName' => 'id4', 'KeyType' => 'RANGE'],
                    ],
                    'Projection' => ['ProjectionType' => 'ALL'],
                    'ProvisionedThroughput' => [
                        'ReadCapacityUnits' => 5,
                        'WriteCapacityUnits' => 5
                    ],
                ]
            ],
            'LocalSecondaryIndexes' => [
                [
                    'IndexName' => 'Index2',
                    'KeySchema' => [
                        ['AttributeName' => 'id5', 'KeyType' => 'HASH'],
                        ['AttributeName' => 'id6', 'KeyType' => 'RANGE']
                    ],
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5
            ]
        ];

        self::$model->addTable($tableDefinition);

        $tableDefinition_reflect = self::$model->descTable('testDB')['Table'];

        $this->assertTrue($tableDefinition === $tableDefinition_reflect);
    }

    public function testDeleteTable()
    {
        self::$model->delTables('testDB');

        return $this->assertTrue(!self::$model->tableExists('testDB', self::$model->getClient()));
    }

    public function testCreateItem()
    {
        $attributes = [
            'id1' => rand(1, 100),
            'id2' => str_random(36),
            'name' => [
                'first' => 'test',
                'last' => 'test1'
            ],
            'count' => 1,
        ];

        self::$model->getSelfInstance('testDB', $attributes)->save();

        $result = self::$model->dall()->all();
        $this->assertEquals($attributes['name']['first'], array_values($result)[0]['name']['first']);
    }

    public function testWhere()
    {
        $attributes = [
            'id1' => 111,
            'id2' => 'dasf231f321das5f32',
            'name' => [
                'first' => 'test',
                'last' => 'test1'
            ],
            'count' => 1,
        ];

        self::$model->getSelfInstance('testDB', $attributes)->save();

//报错，看来不能用路径查询
//        $result = self::$model->where('id1', 111)->where('id2', 'dasf231f321das5f32')->where('name["first"]', 'test')->get()->all();

        $result = self::$model
            ->where('id1', 111)
            ->where('id2', 'dasf231f321das5f32')
            ->get()
            ->all();

        $this->assertTrue(!empty($result));

        $attributes2 = [
            'id1' => 12,
            'id2' => 'dasf231f321das5f32',
            'name' => [
                'first' => 'test',
                'last' => 'test1'
            ],
            'count' => 1,
        ];

        self::$model->getSelfInstance('testDB', $attributes2)->save();

        $result = self::$model->where(function ($query) {
            $query->orWhere('id1', 111);
            $query->orWhere('id1', 12);
        })
            ->where('id2', 'dasf231f321das5f32')
            ->get()
            ->all();

        $this->assertTrue(!empty($result));

        $result = self::$model->orWhere('id1', 111)->orWhere('id2', 'dasf231f321das5f32')->get()->all();
        $this->assertEquals(count($result), 2);

        $result = self::$model->orWhere(function ($query) {
            $query->where('id1', 111);
            $query->where('id2', 'dasf231f321das5f32');
        })
            ->where('id1', 12)
            ->get()
            ->all();

        $this->assertEquals(count($result), 2);


        $result = self::$model
            ->where('id2', '<>', 12)
            ->get()
            ->all();

        $this->assertEquals(count($result), 6);

        $result = self::$model
            ->where('id2', 'BEGINS_WITH', 'das')
            ->get()
            ->all();

        $this->assertEquals(count($result), 2);

        $result = self::$model
            ->where('id2', 'contains', 'das')
            ->get()
            ->all();

        $this->assertEquals(count($result), 2);

        //必须是字符串才行，拼接条件限制
        $result = self::$model
            ->where('id2', 'IN', ['dasf231f321das5f32'])
            ->get()
            ->all();

        $this->assertEquals(count($result), 2);
    }
}