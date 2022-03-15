<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/23
 * Time: 16:26
 */

namespace EsSwoole\Kafka;

use EasySwoole\EasySwoole\Logger;
use EsSwoole\Base\Abstracts\AbstractUserProcess;
use EsSwoole\Base\Exception\ExceptionHandler;
use longlang\phpkafka\Consumer\ConsumeMessage;
use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\ConsumerConfig;
use longlang\phpkafka\Exception\KafkaErrorException;

/**
 * Class AbstrctKafkaConsumeProcess
 *
 * @author dongjw <dongjw.1@jifenn.com>
 */
abstract class AbstrctKafkaConsumeProcess extends AbstractUserProcess
{
    //连接名
    protected $connection;

    //消费名
    protected $consumeName;

    //消费topic
    protected $topic;

    //消费组
    protected $groupId;

    //是否自动提交
    protected $autoCommit;

    //消费者索引
    protected $consumeIndex;

    /**
     * 设置连接
     *
     * @param string $conn
     * User: dongjw
     * Date: 2022/3/15 10:19
     */
    public function setConnection($conn)
    {
        $this->connection = $conn;
    }

    /**
     * 设置topic
     *
     * @param string $topic
     * User: dongjw
     * Date: 2022/3/15 10:19
     */
    public function setTopic($topic)
    {
        $this->topic = $topic;
    }

    /**
     * 消费者组名称
     *
     * @param string $groupId
     * User: dongjw
     * Date: 2022/3/15 10:19
     */
    public function setGroupId($groupId)
    {
        $this->groupId = $groupId;
    }

    /**
     * 设置自动提交
     *
     * @param bool $autoCommit
     * User: dongjw
     * Date: 2022/3/15 10:20
     */
    public function setAutoCommit($autoCommit)
    {
        $this->autoCommit = $autoCommit;
    }

    /**
     * 设置消费索引
     *
     * @param int $index
     * User: dongjw
     * Date: 2022/3/15 10:21
     */
    public function setConsumeIndex($index)
    {
        $this->consumeIndex = $index;
    }

    /**
     * 设置消费者名
     *
     * @param string $name
     * User: dongjw
     * Date: 2022/3/15 10:21
     */
    public function setConsumeName($name)
    {
        $this->consumeName = $name;
    }

    /**
     * 启动消费
     *
     * @param mixed $arg
     * User: dongjw
     * Date: 2022/3/15 10:21
     */
    protected function run($arg)
    {
        try {
            $consumer = new Consumer(
                $this->getConsumeConfig(), function (ConsumeMessage $message) {
                $this->handle($message);
            }
            );
            $consumer->start();
        } catch (KafkaErrorException $e) {
            Logger::getInstance()->error('consume Exception:' . $e->getMessage(), 'kafka');
            ExceptionHandler::report($e);
        }
    }

    /**
     * 获取消费配置
     *
     * @return ConsumerConfig
     * User: dongjw
     * Date: 2022/3/15 10:22
     */
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
        $consumerConfig->setGroupInstanceId(sprintf('%s-%s', $consumeConfig['groupId'], $this->consumeIndex));
        $consumerConfig->setInterval($consumeConfig['interval']);
        $consumerConfig->setClientId(sprintf('%s-%s', $consumeConfig['groupId'], $this->consumeIndex));
        $consumerConfig->setSessionTimeout($consumeConfig['sessionTimeout']);
        $consumerConfig->setGroupHeartbeat($consumeConfig['groupHeartbeat']);
        $consumerConfig->setMinBytes($consumeConfig['minBytes']);
        $consumerConfig->setMaxBytes($consumeConfig['maxBytes']);
        $consumerConfig->setPartitionAssignmentStrategy($consumeConfig['partitionAssignmentStrategy']);

        return $consumerConfig;
    }

    /**
     * 消费逻辑
     *
     * @param ConsumeMessage $message
     *
     * @return mixed
     * User: dongjw
     * Date: 2022/3/15 10:22
     */
    abstract public function handle(ConsumeMessage $message);
}
