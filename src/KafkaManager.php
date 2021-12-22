<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/22
 * Time: 18:46
 */

namespace EsSwoole\Kafka;


use EasySwoole\EasySwoole\Logger;
use EasySwoole\Pool\Manager;
use EsSwoole\Kafka\Producer;

class KafkaManager
{
    public static function send(string $topic, string $value, string $key = null, array $headers = [], int $partitionIndex = null, $conn = 'default')
    {
        $pool = Manager::getInstance()->get($conn);
        if (!$pool) {
            throw new \Exception("{$conn} kafka连接池为空");
        }
        return $pool->invoke(function (Producer $producer) use($topic, $value, $key, $headers, $partitionIndex){
            return $producer->push($topic, $value, $key, $headers, $partitionIndex);
        });
    }

    public static function sendBatch(array $messages, $conn = 'default')
    {
        $pool = Manager::getInstance()->get($conn);
        if (!$pool) {
            throw new \Exception("{$conn} kafka连接池为空");
        }
        return $pool->invoke(function (Producer $producer) use($messages){
            return $producer->pushBatch($messages);
        });
    }
}