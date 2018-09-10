<?php
declare(strict_types=1);

namespace icePHP;

/**
 * 创建redis 表对象
 * 这是SRedis的一个快捷入口
 * @param string $name 表名
 * @param mixed $orderBy 需要排序索引的字段
 * @return RedisTable
 */
function redis(string $name, $orderBy = []): RedisTable
{
    return new RedisTable($name, $orderBy);
}