<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/23
 * Time: 16:26
 */

namespace EsSwoole\Kafka;


use EsSwoole\Base\Abstracts\AbstractUserProcess;
use longlang\phpkafka\Consumer\ConsumeMessage;
use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\ConsumerConfig;

abstract class AbstrctKafkaConsumeProcess extends AbstractUserProcess
{
    protected $connection;

    protected $consumeName;

    protected $topic;

    protected $groupId;

    protected $autoCommit;

    protected $consumeIndex;

    public function setConnection($conn)
    {
        $this->connection = $conn;
    }

    public function setTopic($topic)
    {
        $this->topic = $topic;
    }

    public function setGroupId($groupId)
    {
        $this->groupId = $groupId;
    }

    public function setAutoCommit($autoCommit)
    {
        $this->autoCommit = $autoCommit;
    }

    public function setConsumeIndex($index)
    {
        $this->consumeIndex = $index;
    }

    public function setConsumeName($name)
    {
        $this->consumeName = $name;
    }

    protected function run($arg)
    {
        $consumer = new Consumer($this->getConsumeConfig(), function (ConsumeMessage $message){
            $this->handle($message);
        });
        $consumer->start();
    }

    protected function getConsumeConfig()
    {
        //连接配置
        $connConfig = config("kafka.connections.{$this->connection}");
        //消费配置
        $consumeConfig = config("kafka.consumer.{$this->consumeName}");

        $consumerConfig = new ConsumerConfig();
        $consumerConfig->setBootstrapServers($connConfig['brokers']);
        $consumerConfig->setConnectTimeout($connConfig['connectTimeout']);
        $consumerConfig->setSendTimeout($connConfig['sendTimeout']);

        $consumerConfig->setAutoCommit($this->autoCommit);
        $consumerConfig->setTopic($this->topic);
        $consumerConfig->setRebalanceTimeout($consumeConfig['rebalanceTimeout']);
        $consumerConfig->setGroupId($consumeConfig['groupId']);
        $consumerConfig->setGroupInstanceId(sprintf('%s-%s', $consumeConfig['groupId'],$this->consumeIndex));
        $consumerConfig->setInterval($consumeConfig['interval']);
        $consumerConfig->setClientId(sprintf('%s-%s', $consumeConfig['groupId'], $this->consumeIndex));
        $consumerConfig->setSessionTimeout($consumeConfig['sessionTimeout']);
        $consumerConfig->setGroupHeartbeat($consumeConfig['groupHeartbeat']);
        $consumerConfig->setMinBytes($consumeConfig['minBytes']);
        $consumerConfig->setMaxBytes($consumeConfig['maxBytes']);
        $consumerConfig->setPartitionAssignmentStrategy($consumeConfig['partitionAssignmentStrategy']);
        return $consumerConfig;
    }

    abstract public function handle(ConsumeMessage $message);
}