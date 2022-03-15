<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/22
 * Time: 18:46
 */

namespace EsSwoole\Kafka;

use EasySwoole\Pool\Manager;

/**
 * Class KafkaManager
 *
 * @author dongjw <dongjw.1@jifenn.com>
 */
class KafkaManager
{
    /**
     * push单条消息
     *
     * @param string      $topic
     * @param string      $value
     * @param string|null $key            如果有此值,会根据该值hash push到hash计算后的分区
     * @param array       $headers
     * @param int|null    $partitionIndex push到指定的分区
     * @param string      $conn           指定使用哪个连接
     *
     * @return bool
     * User: dongjw
     * Date: 2021/12/27 11:13
     */
    public static function send(string $topic, string $value, string $key = null, array $headers = [], int $partitionIndex = null, $conn = 'default')
    {
        $pool = Manager::getInstance()->get(self::getPoolName($conn));
        if (!$pool) {
            throw new \Exception("{$conn} kafka连接池为空");
        }

        return $pool->invoke(
            function (Producer $producer) use ($topic, $value, $key, $headers, $partitionIndex) {
                $producer->push($topic, $value, $key, $headers, $partitionIndex);
            }
        );
    }

    /**
     * 批量发送消息
     *
     * @param array  $messages 批量消息体
     * @param string $conn     指定使用哪个连接
     *
     * @return bool
     * User: dongjw
     * Date: 2021/12/27 11:15
     */
    public static function sendBatch(array $messages, $conn = 'default')
    {
        $pool = Manager::getInstance()->get(self::getPoolName($conn));
        if (!$pool) {
            throw new \Exception("{$conn} kafka连接池为空");
        }

        return $pool->invoke(
            function (Producer $producer) use ($messages) {
                $producer->pushBatch($messages);
            }
        );
    }

    /**
     * 获取连接池名
     *
     * @param $conn
     *
     * @return string
     * User: dongjw
     * Date: 2022/3/15 9:59
     */
    public static function getPoolName($conn)
    {
        return 'kafka:' . $conn;
    }
}
