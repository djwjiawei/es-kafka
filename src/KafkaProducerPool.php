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

/**
 * Class KafkaProducerPool
 *
 * @author dongjw <dongjw.1@jifenn.com>
 */
class KafkaProducerPool extends AbstractPool
{
    //连接名
    protected $conn;

    /**
     * KafkaProducerPool constructor.
     *
     * @param string $conn
     * @param Config $conf
     */
    public function __construct($conn, Config $conf)
    {
        $this->conn = $conn;
        parent::__construct($conf);
    }

    /**
     * 获取Producer对象
     *
     * @return Producer
     * User: dongjw
     * Date: 2022/3/15 10:28
     */
    protected function createObject()
    {
        $config         = \config("kafka.connections.{$this->conn}");
        $producerConfig = new ProducerConfig();
        $producerConfig->setConnectTimeout($config['connectTimeout']);
        $producerConfig->setSendTimeout($config['sendTimeout']);
        $producerConfig->setRecvTimeout($config['recvTimeout']);
        $producerConfig->setBootstrapServers($config['brokers']);
        $producerConfig->setAcks($config['acks']);

        return new Producer($producerConfig);
    }

}
