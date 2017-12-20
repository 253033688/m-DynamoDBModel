<?php

namespace App\Services\DynamoDB;

class EmptyAttributeFilter
{
    public function filter(&$store)
    {
        foreach ($store as $key => &$value) {
            $value = is_string($value) ? trim($value) : $value;
            $empty = $value === null || (is_array($value) && empty($value));

            $empty = $empty || (is_scalar($value) && $value !== false && (string)$value === '');

            if ($empty) {
                $store[$key] = null;
            } else {
                if (is_object($value)) {
                    $value = (array)$value;
                }
                if (is_array($value)) {
                    $this->filter($value);
                }
            }
        }
    }
}
