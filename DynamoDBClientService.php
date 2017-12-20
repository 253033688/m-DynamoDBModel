<?php

namespace App\Services\DynamoDB;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;

class DynamoDBClientService
{
    protected $client;
    protected $marshaler;
    protected $attributeFilter;

    public function __construct($config, Marshaler $marshaler, EmptyAttributeFilter $filter)
    {
        $this->client = new DynamoDbClient($config);
        $this->marshaler = $marshaler;
        $this->attributeFilter = $filter;
    }

    public function getClient()
    {
        return $this->client;
    }

    public function getMarshaler()
    {
        return $this->marshaler;
    }

    public function getAttributeFilter()
    {
        return $this->attributeFilter;
    }
}