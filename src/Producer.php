<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/22
 * Time: 16:32
 */

namespace EsSwoole\Kafka;


use EasySwoole\EasySwoole\Logger;
use longlang\phpkafka\Producer\Producer as LongLangProducer;

class Producer extends LongLangProducer
{
    public function push(string $topic, string $value, string $key = null, array $headers = [], int $partitionIndex = null)
    {
        try {
            $this->send($topic, $value, $key, $headers, $partitionIndex);
            return true;
        } catch (\Throwable $e) {
            Logger::getInstance()->error("topic:{$topic};value:{$value}; exception:{$e->getMessage()}","kafka");
            //更新一下broker列表,以防有新的broker节点
            $this->getBroker()->updateBrokers();
            //异常后关闭socket,下次重新连接
            $this->close();
            return false;
        }
    }

    public function pushBatch(array $messages)
    {
        try {
            $this->sendBatch($messages);
            return true;
        } catch (\Throwable $e) {
            $log = [];
            foreach ($messages as $msgObj) {
                $log[] = [
                    'topic' => $msgObj->getTopic(),
                    'value' => $msgObj->getValue()
                ];
            }
            Logger::getInstance()->error("sendBatch exception:{$e->getMessage()}" . " --- " . json_encode($log),"kafka");
            //更新一下broker列表,以防有新的broker节点
            $this->getBroker()->updateBrokers();
            //异常后关闭socket,下次重新连接
            $this->close();
            return false;
        }
    }
}