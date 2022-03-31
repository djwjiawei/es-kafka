<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/23
 * Time: 16:26
 */

namespace EsSwoole\Kafka;

use EasySwoole\EasySwoole\Logger;
use EasySwoole\Component\Process\AbstractProcess;
use EsSwoole\Base\Exception\ExceptionHandler;
use EsSwoole\Base\Util\AppUtil;
use longlang\phpkafka\Consumer\ConsumeMessage;
use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\ConsumerConfig;
use longlang\phpkafka\Exception\KafkaErrorException;
use Swoole\Event;
use Swoole\Process;

/**
 * Class AbstrctKafkaConsumeProcess
 *
 * @author dongjw <dongjw.1@jifenn.com>
 */
abstract class AbstrctKafkaConsumeProcess extends AbstractProcess
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

    //最大重试次数
    protected $maxRetryTimes = 3;

    //每次重试间隔时间(毫秒)
    protected $retrySleep = 100;

    /**
     * Consumer对象
     *
     * @var Consumer
     */
    protected $consumer;

    protected $isClose = false;

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
        //监听term信号,主要为了容器关闭时 离开组 触发再平衡
        $this->listenTerm();

        $retryTimes = 0;

        try {
            beginning:
            Logger::getInstance()->info($this->getProcessName() . '开始run....');

            $hasInit = false;

            $this->consumer = new Consumer(
                $this->getConsumeConfig(), function (ConsumeMessage $message) {
                $this->handle($message);
            }
            );

            $hasInit = true;

            $this->isClose = false;

            Logger::getInstance()->info($this->getProcessName() . '初始化结束....');

            $this->consumer->start();
        } catch (\Throwable $e) {
            $processName = $this->getProcessName() ?: '';
            Logger::getInstance()->error($processName . 'consume Exception:' . $e->getMessage());

            //发送报告
            ExceptionHandler::report($e);

            //如果已经初始化过,则先关闭消费者 离开组
            if ($hasInit && !$this->isClose) {
                $this->consumer->close();
                $this->isClose = true;
            }

            //将consumer设为null
            $this->consumer = null;

            if ($retryTimes <= $this->maxRetryTimes) {
                //未达到最大重试次数 继续重试
                $retryTimes++;
                usleep($this->retrySleep * 1000);
                //重新start
                goto beginning;
            } else {
                //终止进程,否则进程会一直挂起 不消费
                \Swoole\Process::kill(posix_getpid());
            }
        }
    }

    /**
     * 监听term信号 调用离开组
     * User: dongjw
     * Date: 2022/3/31 16:25
     */
    protected function listenTerm()
    {
        Process::signal(
            SIGTERM, function () {
            //清除进程pipe事件
            swoole_event_del($this->getProcess()->pipe);

            //清除信号监听
            Process::signal(SIGTERM, null);

            //信号方法里 协程环境已经不存在了 要新开一个go
            go(function (){
                if (!$this->isClose && $this->consumer) {
                    $this->consumer->close();

                    //标识为已关闭 防止run方法继续执行
                    $this->isClose = true;
                    Logger::getInstance()->info($this->getProcessName() . '进程term退出 close结束');

                    //清除资源
                    \Swoole\Timer::clearAll();
                    Event::exit();
                }
                Logger::getInstance()->info($this->getProcessName() . '进程term退出');
            });
        }
        );
    }

    /**
     * 进程退出调用离开组
     * User: dongjw
     * Date: 2022/3/30 18:56
     */
    protected function onSigTerm()
    {
        if ($this->isClose) {
            $this->consumer->close();
            Logger::getInstance()->info($this->getProcessName() . '进程term退出 close结束');
        }
        Logger::getInstance()->info($this->getProcessName() . '进程term退出');
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
        $consumerConfig->setGroupInstanceId(sprintf('%s-%s-%s', $consumeConfig['groupId'], AppUtil::getLocalIp(), $this->consumeIndex));
        $consumerConfig->setInterval($consumeConfig['interval']);
        $consumerConfig->setClientId(sprintf('%s-%s-%s', $consumeConfig['groupId'], AppUtil::getLocalIp(), $this->consumeIndex));
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
