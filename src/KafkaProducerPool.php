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
        $config = \config("kakfa.connections.{$this->conn}");
        $producerConfig = new ProducerConfig();
        $producerConfig->setConnectTimeout($config['connect_timeout']);
        $producerConfig->setSendTimeout($config['send_timeout']);
        $producerConfig->setRecvTimeout($config['recv_timeout']);
        $producerConfig->setClientId($config['client_id']);
        $producerConfig->setBootstrapServers($config['bootstrap_servers']);
        $producerConfig->setAcks($config['acks']);
        return new Producer($producerConfig);
    }

}