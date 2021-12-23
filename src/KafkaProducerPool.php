<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/22
 * Time: 14:15
 */

namespace EsSwoole\Kafka;


use EasySwoole\Pool\AbstractPool;
use EasySwoole\Pool\Config;
use longlang\phpkafka\Producer\ProducerConfig;

class KafkaProducerPool extends AbstractPool
{
    protected $conn;

    public function __construct($conn, Config $conf)
    {
        $this->conn = $conn;
        parent::__construct($conf);
    }

    protected function createObject()
    {
        $config = \config("kafka.connections.{$this->conn}");
        $producerConfig = new ProducerConfig();
        $producerConfig->setConnectTimeout($config['connectTimeout']);
        $producerConfig->setSendTimeout($config['sendTimeout']);
        $producerConfig->setRecvTimeout($config['recvTimeout']);
        $producerConfig->setBootstrapServers($config['brokers']);
        $producerConfig->setAcks($config['acks']);
        return new Producer($producerConfig);
    }

}