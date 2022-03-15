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

/**
 * Class Producer
 *
 * @author dongjw <dongjw.1@jifenn.com>
 */
class Producer extends LongLangProducer
{
    /**
     * 发送单条消息
     *
     * @param string      $topic
     * @param string      $value
     * @param string|null $key
     * @param array       $headers
     * @param int|null    $partitionIndex
     *
     * @throws \Throwable
     * User: dongjw
     * Date: 2022/3/15 10:26
     */
    public function push(string $topic, string $value, string $key = null, array $headers = [], int $partitionIndex = null)
    {
        try {
            $this->send($topic, $value, $key, $headers, $partitionIndex);
        } catch (\Throwable $e) {
            Logger::getInstance()->error(
                "sendOne Exception: topic-{$topic};value-{$value}; exception-{$e->getMessage()}", "kafka"
            );
            //更新一下broker列表,以防有新的broker节点
            //$this->getBroker()->updateBrokers();
            //异常后关闭socket 下次重新连接(新版已经做重连 这里不再close)
            //$this->close();
            throw $e;
        }
    }

    /**
     * 批量发送消息
     *
     * @param array $messages
     *
     * @throws \Throwable
     * User: dongjw
     * Date: 2022/3/15 10:27
     */
    public function pushBatch(array $messages)
    {
        try {
            $this->sendBatch($messages);
        } catch (\Throwable $e) {
            $log = [];
            foreach ($messages as $msgObj) {
                $log[] = [
                    'topic' => $msgObj->getTopic(),
                    'value' => $msgObj->getValue(),
                ];
            }
            Logger::getInstance()->error(
                "sendBatch exception:{$e->getMessage()}" . " --- " . json_encode($log), "kafka"
            );
            //更新一下broker列表,以防有新的broker节点
            //$this->getBroker()->updateBrokers();
            //异常后关闭socket 下次重新连接(新版已经做重连 这里不再close)
            //$this->close();
            throw $e;
        }
    }
}
