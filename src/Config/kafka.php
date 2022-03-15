<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/22
 * Time: 14:22
 */

return [
    //kafka连接配置
    'connections' => [
        'default' => [
            //broker节点,支持"192.168.0.1,192,168.0.2"或["192.168.0.1","192.168.0.2"]两种写法
            'brokers' => '',
            //socket连接超时时间
            'connectTimeout' => 3,
            //socket send超时时间
            'sendTimeout' => 3,
            //socket recv超时时间
            'recvTimeout' => 3,
            //producer 写消息后的确认机制,0为不等待broker的响应 1为等待leader的响应 -1为等待leader和follwer的响应
            'acks' => 1
        ]
    ],
    //是否启动消费进程
    'isConsume' => true,
    //consume配置
    'consumer' => [
//        'exchange' => [
//            //消费者进程类,需要继承AbstrctKafkaConsumeProcess类
//            'processClass' => '',
//            //消费者进程数,建议和topic的分区数一样,不能大于topic的分区数
//            'processNums' => 1,
//            //要消费的topic,支持字符串或字符串数组
//            'topic' => '',
//            //消费者组名称
//            'groupId' => '',
//            //是否自动提交
//            'autoCommit' => true,
//            //拉不到消息时 sleep的时间单位/秒,支持小数
//            'interval' => 1,
//            //组协调器接收心跳超时时间,超过这个时间没发心跳,则认为这个消费者下线
//            'sessionTimeout' => 120,
//            //心跳间隔时间,要小于sessionTimeout
//            'groupHeartbeat' => 10,
//            //再平衡时,等待加入组的时间
//            'rebalanceTimeout' => 20,
//            //拉取的最小字节数
//            'minBytes' => 1,
//            //拉取的最大字节数
//            'maxBytes' => 5 * 1024 * 1024,
//            //消费分区分配策略
//            'partitionAssignmentStrategy' => \longlang\phpkafka\Consumer\Assignor\RangeAssignor::class,
//            //对应上边配置的connections的连接
//            'connection' => 'default'
//        ]
    ],
    //连接池配置
    'pool' => [
        //连接池中获取对象超时时间
        'getObjTimeout' => 3,
        //定时任务检查空闲连接和最小连接间隔时间,单位/毫秒
        'intervalCheck' => 30 * 1000,
        //连接最大空闲时间
        'maxIdleTime' => 120,
        //连接池最小数量
        'minObjNum' => 1,
        //连接池最大数量
        'maxObjNum' => 5
    ]
];