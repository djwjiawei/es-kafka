<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/22
 * Time: 16:19
 */

namespace EsSwoole\Kafka;

use EasySwoole\Pool\Config;
use EasySwoole\Pool\Manager;
use EsSwoole\Base\Abstracts\AbstractProvider;

/**
 * Class KafkaProvider
 *
 * @author dongjw <dongjw.1@jifenn.com>
 */
class KafkaProvider extends AbstractProvider
{

    /**
     * Register
     * User: dongjw
     * Date: 2022/3/15 10:30
     */
    public function register()
    {
        //注入kafka连接池
        $config = \config('kafka.connections');
        foreach ($config as $conn => $data) {
            $this->registerProducerPool($conn);
        }
    }

    /**
     * 注入连接池
     *
     * @param string $conn
     * User: dongjw
     * Date: 2022/3/15 10:30
     */
    public function registerProducerPool($conn)
    {
        $poolName = KafkaManager::getPoolName($conn);
        if (!Manager::getInstance()->get($poolName)) {
            $poolInfo = \config('kafka.pool');

            $poolConfig = new Config();
            $poolConfig->setGetObjectTimeout($poolInfo['getObjTimeout'] ?: 3); //设置获取连接池对象超时时间
            $poolConfig->setIntervalCheckTime($poolInfo['intervalCheck'] ?: 30000); //设置检测连接存活执行回收和创建的周期
            $poolConfig->setMaxIdleTime($poolInfo['maxIdleTime'] ?: 60); //连接池对象最大闲置时间(秒)
            $poolConfig->setMinObjectNum($poolInfo['minObjNum'] ?: 1); //设置最小连接池存在连接对象数量
            $poolConfig->setMaxObjectNum($poolInfo['maxObjNum'] ?: 5); //设置最大连接池存在连接对象数量

            Manager::getInstance()->register(
                new KafkaProducerPool($conn, $poolConfig), $poolName
            );
        }
    }

    /**
     * Boot
     *
     * User: dongjw
     * Date: 2022/3/15 10:30
     */
    public function boot()
    {
        $kafkaConfig = \config('kafka');
        if (!$kafkaConfig['isConsume']) {
            return;
        }

        //启动consumer
        $consumers = $kafkaConfig['consumer'];
        if (!$consumers) {
            return;
        }

        foreach ($consumers as $consumeName => $consumeConfig) {
            if (!$consumeConfig) {
                continue;
            }
            //消费者组所用连接
            $consumeConn = $consumeConfig['connection'];
            if (!\config("kafka.connections.{$consumeConn}")) {
                throw new \Exception("没有{$consumeConn}连接配置");
            }
            //启动对应数量的消费进程
            for ($i = 0; $i < $consumeConfig['processNums']; $i++) {
                $processConfig  = new \EasySwoole\Component\Process\Config(
                    [
                        'processName'     => "Kafka.Consume.{$consumeName}_{$i}", //设置进程名称
                        'processGroup'    => 'Kafka.Consume', //设置进程组名称
                        'enableCoroutine' => true, //设置开启协程
                    ]
                );
                $consumeProcess = new $consumeConfig['processClass']($processConfig);

                if (!($consumeProcess instanceof AbstrctKafkaConsumeProcess)) {
                    break;
                }
                $consumeProcess->setConnection($consumeConfig['connection']);
                $consumeProcess->setTopic($consumeConfig['topic']);
                $consumeProcess->setGroupId($consumeConfig['groupId']);
                $consumeProcess->setAutoCommit($consumeConfig['autoCommit']);
                $consumeProcess->setConsumeIndex($i);
                $consumeProcess->setConsumeName($consumeName);

                \EasySwoole\Component\Process\Manager::getInstance()->addProcess($consumeProcess);
            }
        }
    }

}
