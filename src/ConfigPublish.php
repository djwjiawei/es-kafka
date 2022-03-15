<?php
/**
 * Created by PhpStorm.
 * User: dongjw
 * Date: 2021/11/24
 * Time: 13:51
 */

namespace EsSwoole\kafka;

use EsSwoole\Base\Abstracts\ConfigPublishInterface;

/**
 * Class ConfigPublish
 *
 * @author dongjw <dongjw.1@jifenn.com>
 */
class ConfigPublish implements ConfigPublishInterface
{
    /**
     * 发布配置
     *
     * @return array
     * User: dongjw
     * Date: 2022/3/15 10:33
     */
    public function publish()
    {
        return [
            __DIR__ . '/config/kafka.php' => configPath('kafka.php'),
        ];
    }
}
