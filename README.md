# easyswoole nacos包

## 安装包
```
1. 在composer.json中添加该配置
"repositories": [
    {
        "type": "git",
        "url": "git@git.dev.enbrands.com:X/php/interaction/easyswoole_kafka.git"
    }
]

2. 执行composerrequire
composer require es-swoole/kafka:(dev-master或具体tag)
```

## 已开发功能
- [x] 生产者连接池
- [x] 根据配置生成consume进程

## 使用步骤
1. 安装本包(composer require es-swoole/nacos:dev-master或具体tag)
2. 在根目录的bootstrap文件中添加:
\EasySwoole\Command\CommandManager::getInstance()->addCommand(new \EsSwoole\Base\Command\PublishConfig());  用于发布vendor包配置 
5. 执行php easyswoole publish:config --vendor=es-swoole/kafka 发布本包配置
6. 修改发布的配置（config/kafka.php）
8. 在EasySwooleEvent的initialize方法中添加ServiceProvider::getInstance()->registerVendor(); 用户初始化vendor包的服务提供者
9. 在EasySwooleEvent的mainServerCreate方法中添加ServiceProvider::getInstance()->bootrVendor(); 用户启动vendor包的服务提供者


## 生产者使用方法
```php
/**
 * push单条消息
 * @param string $topic push的topic
 * @param string $value push的消息
 * @param string|null $key 如果有此值,会根据该值hash push到hash计算后的分区
 * @param array $headers
 * @param int|null $partitionIndex push到指定的分区
 * @param string $conn 指定使用哪个连接
 * @return bool
 */
\EsSwoole\Kafka\KafkaManager::send('chat',time());

/**
 * 批量发送消息
 * @param array $messages 批量消息体
 * @param string $conn 指定使用哪个连接
 * @return bool
 */
\EsSwoole\Kafka\KafkaManager::sendBatch([
    new \longlang\phpkafka\Producer\ProduceMessage('chat', 'v1'),
    new \longlang\phpkafka\Producer\ProduceMessage('news', 'v2'),
],'default');
```

## 消费者使用方法
```
1. 创建消费进程类继承AbstrctKafkaConsumeProcess类,实现handle方法
public function handle(ConsumeMessage $message)
{
    //该条消息的topic
    $topic = $message->getTopic();
    //该条消息的value
    $value = $message->getValue();
    //该条消息的partition
    $partition = $message->getPartition();
}
2. 在config/kafka.php中consumer内指定消费配置
```