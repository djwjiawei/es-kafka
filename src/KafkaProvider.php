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

class KafkaProvider extends AbstractProvider
{

    public function register()
    {
        //注入kafka连接池
        $config = \config('kafka.connections');
        foreach ($config as $conn => $data){
            $this->registerProducerPool($conn);
        }
    }

    public function registerProducerPool($conn)
    {
        $poolName = $this->getPoolName($conn);
        if (!Manager::getInstance()->get($poolName)) {
            $poolInfo = \config('kafka.pool');
            
            $poolConfig = new Config();
            $poolConfig->setGetObjectTimeout($poolInfo['getObjTimeout'] ?: 3); //设置获取连接池对象超时时间
            $poolConfig->setIntervalCheckTime($poolInfo['intervalCheck'] ?: 30000); //设置检测连接存活执行回收和创建的周期
            $poolConfig->setMaxIdleTime($poolInfo['maxIdleTime'] ?: 60); //连接池对象最大闲置时间(秒)
            $poolConfig->setMinObjectNum($poolInfo['minObjNum'] ?: 1); //设置最小连接池存在连接对象数量
            $poolConfig->setMaxObjectNum($poolInfo['maxObjNum'] ?: 20); //设置最大连接池存在连接对象数量
            Manager::getInstance()->register(
                new KafkaProducerPool($conn, $poolConfig),
                $poolName
            );
        }
    }

    public function getPoolName($conn)
    {
        return 'kafka:' . $conn;
    }

    public function boot()
    {
        //todo 启动consumer
    }

}