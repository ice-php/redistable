<?php
declare(strict_types=1);

namespace icePHP;

/**
 * Redis表对象,使用String保存ID,使用Hash保存行,使用ZSet建立索引
 * User: 蓝冰
 * Date: 2017/7/27
 */
class RedisTable
{
    //表名,用来生成键的前缀
    private $name;

    /**
     * @var array 所有需要索引的字段
     */
    private $orderBy = [];

    /**
     * 指定表名和排序,创建一个表对象
     * @param $name string 表名
     * @param $orderBy string|array 需要索引的字段(列表)
     */
    public function __construct($name, $orderBy = null)
    {
        //表名(实际以前缀表示)
        $this->name = $name;

        //需要索引的字段名,以数组形式存储
        if ($orderBy and !is_array($orderBy)) {
            $orderBy = [$orderBy];
        }

        $this->orderBy = $orderBy;
    }

    /**
     * 获取索引表对象(zSet)
     * @param $orderBy string 排序字段
     * @return RedisSortedSet
     */
    private function index($orderBy)
    {
        //REDIS的键,
        $key = $this->name . ':INDEX:' . $orderBy;

        //获取索引表,名称例: user_INDEX_birth
        $index = Redis::get($key);

        //必须是sortedSet类型
        if ($index instanceof RedisSortedSet) {
            return $index;
        }

        //索引表尚未建立
        return Redis::createSortedSet($key);
    }

    /**
     * 获取数据表对象
     * @return RedisHash
     */
    private function data()
    {
        //REDIS中的键
        $key = $this->name . ':DATA';

        //取出数据
        $data = Redis::get($key);

        //必须是HASH表
        if ($data instanceof RedisHash) {
            return $data;
        }

        //创建新的HASH表
        return Redis::createHash($key);
    }

    /**
     * 获取自增长ID对象
     * @return RedisInt
     */
    private function id()
    {
        //REDIS中的键
        $key = $this->name . ':ID';

        //取出
        $id = Redis::get($key);
        if ($id instanceof RedisString) {
            return $id->toInt();
        }

        //创建新的
        return Redis::createInt($key, 0);
    }

    /**
     * 查询
     * @param $orderBy string  排序的字段
     * @param $min float|int 下限
     * @param $max float}int 上限
     * @param $offset int 分页偏移
     * @param $length int 分页长度
     * @param bool $desc 是否降序
     * @return Result
     * @throws RedisTableException
     */
    public function select(string $orderBy, $min, $max, int $offset, int $length, bool $desc = false): Result
    {
        //必须是创建表时指定的排序字段
        if (!in_array($orderBy, $this->orderBy)) {
            throw new RedisTableException('排序字段必须事先定义:' . $orderBy, RedisTableException::UNDEFINED_ORDER_FIELD);
        }

        //取索引
        $index = $this->index($orderBy);
        $ids = $index->rangeByScore($min, $max, [$offset, $offset + $length], false, $desc);

        //根据索引取数据
        $result = $this->data()->multiGet($ids);

        //JSON解析
        $ret = [];
        foreach ($result as $k => $row) {
            $ret[$k] = json_decode($row, true);
        }

        return new Result($this->name, $ret);
    }

    /**
     * 插入一行数据
     * @param array $row 要插入的数据
     * @return int 新的ID
     */
    public function insert(array $row)
    {
        //创建自增长ID,并加入数据中
        $idObj = $this->id();
        try {
            $newId = $idObj->crease();
        } catch (RedisException $e) {
            trigger_error('不可能到达这里', E_USER_ERROR);
            exit;
        }
        $row['redisId'] = $newId;

        //以JSON格式存储到HASH表中
        $dataObj = $this->data();
        $dataObj->set(strval($newId), json($row));

        //添加索引
        foreach ($this->orderBy as $orderBy) {
            $indexObj = $this->index($orderBy);
            $indexObj->add($row[$orderBy], $newId);
        }

        //返回ID
        return $newId;
    }

    /**
     * 取一行数据
     * @param $id int REDIS编号
     * @return Row
     */
    public function row($id)
    {
        $dataObj = $this->data();
        $row = json_decode($dataObj->get($id), true);

        return new Row($this->name, $row);
    }

    /**
     * 更新一行数据
     * @param $id int 行编号
     * @param array $row 行数据
     * @return int 行编号
     */
    public function update($id, array $row)
    {
        //取出原数据
        $old = $this->row($id)->toArray();

        //合并新数据
        $row = array_merge($old, $row);

        //保存数据
        $dataObj = $this->data();
        $dataObj->set(strval($id), json($row));

        //保存索引
        foreach ($this->orderBy as $orderBy) {
            $indexObj = $this->index($orderBy);
            $indexObj->add($row[$orderBy], $id);
        }
        return $id;
    }

    /**
     * 删除一行数据
     * @param $id int 行编号
     */
    public function delete($id)
    {
        //删除数据
        $dataObj = $this->data();
        $dataObj->deleteField($id);

        //删除索引
        foreach ($this->orderBy as $orderBy) {
            $indexObj = $this->index($orderBy);
            $indexObj->remove($id);
        }
    }
}