<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/12/22
 * Time: 14:22
 */

return [
    'connections' => [
        'default' => [
            'bootstrap_servers' => '',
            'connect_timeout' => 3,
            'send_timeout' => 3,
            'recv_timeout' => 3,
            //producer 写消息后的确认机制,0为不等待broker的响应 1为等待leader的响应 -1为等待leader和follwer的响应
            'acks' => 1,
            'produceRetry' => 3,
            'produceRetrySleep' => 0.1
        ]
    ],
    'pool' => [
        'getObjTimeout' => 3,
        'intervalCheck' => 30000,
        'maxIdleTime' => 120,
        'minObjNum' => 1,
        'maxObjNum' => 5
    ]
];