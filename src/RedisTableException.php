<?php
declare(strict_types=1);

namespace icePHP;

class RedisTableException extends \Exception
{
    //排序字段必须事先定义
    const UNDEFINED_ORDER_FIELD = 1;
}