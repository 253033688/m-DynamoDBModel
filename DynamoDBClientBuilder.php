<?php

namespace App\Services\DynamoDB;

use Aws\DynamoDb\Marshaler;

class DynamoDBClientBuilder
{
    protected $client;
    protected $marshaler;
    protected $attributeFilter;

    private static $dynamoDb = null;

    //建立dynamodb连接
    public function __construct()
    {
        if (is_null(static::$dynamoDb)) {
            if (config('services.dynamodb.local')) {
                $config = [
                    'credentials' => [
                        'key' => config('services.dynamodb.local_key'),
                        'secret' => config('services.dynamodb.local_secret'),
                    ],
                    'region' => config('services.dynamodb.local_region'),
                    'version' => 'latest',
                    'endpoint' => config('services.dynamodb.local_endpoint'),
                ];
            } else {
                $config = [
                    'credentials' => [
                        'key' => config('services.dynamodb.key'),
                        'secret' => config('services.dynamodb.secret'),
                    ],
                    'region' => config('services.dynamodb.region'),
                    'version' => 'latest',
                ];
            }

            static::$dynamoDb = new DynamoDBClientService($config, new Marshaler(), new EmptyAttributeFilter());

            $this->client = static::$dynamoDb->getClient();
            $this->marshaler = static::$dynamoDb->getMarshaler();
            $this->attributeFilter = static::$dynamoDb->getAttributeFilter();
        }
    }

    /*获取dynamodb句柄*/
    public function getClient()
    {
        return $this->client;
    }

    /*设置dynamodb句柄*/
    public function setClient($client)
    {
        $this->client = $client;
    }

    /*类型转换php->dynamodb*/
    public function getMarshal()
    {
        return $this->marshaler;
    }

    /*类型转换dynamodb->php*/
    public function setMarshal($marshaler)
    {
        return $this->marshaler = $marshaler;
    }

    public function getAttributeFilter()
    {
        return $this->attributeFilter;
    }

}