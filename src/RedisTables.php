<?php

namespace IMEdge\RedisTables;

use Amp\Redis\RedisClient;
use gipfl\Json\JsonString;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;
use Psr\Log\LoggerInterface;

class RedisTables
{
    public const STREAM_NAME_PREFIX = 'db-stream-';

    protected LuaScriptRunner $lua;
    protected string $streamName;

    public function __construct(
        string $streamNameSuffix,
        protected readonly RedisClient $redis,
        protected readonly LoggerInterface $logger,
    ) {
        $this->streamName = self::STREAM_NAME_PREFIX . $streamNameSuffix;
        $this->lua = new LuaScriptRunner($this->redis, dirname(__DIR__) . '/lua', $this->logger);
    }

    public function getTable(string $table): mixed
    {
        $result = $this->lua->runScript('getTable', [$this->streamName, $table]);
        $this->logger->notice(var_export($result, 1));
        if ($result !== null) {
            $result[1] = array_map(JsonString::decode(...), RedisResult::toArray($result[1]));
        }

        return $result;
    }

    public function setTableForDevice(
        string $table,
        string $devicePrefix,
        array $keyProperties,
        array $tables
    ) {
        $tables = array_map(self::createTableEntry(...), $tables);

        return RedisResult::toHash($this->lua->runScript('setTable', [
            $this->streamName,
            $table,
            40, // strlen($checksum)
            $devicePrefix,
            JsonString::encode($keyProperties),
        ], self::arrayToLuaTable($tables)))->status;
    }

    public function setTableEntry(
        string $table,
        string $key,
        array $keyProperties,
        mixed $data
    ) {
        return RedisResult::toHash($this->lua->runScript('setTableEntry', [
            $this->streamName,
            $table,
            40, // strlen($checksum)
            $key,
            JsonString::encode($keyProperties)
        ], [
            self::createTableEntry($data),
        ]))->status;
    }

    protected static function createTableEntry($row): string
    {
        $json = JsonString::encode($row);
        return sha1($json) . $json;
    }

    protected static function arrayToLuaTable(array $array): array
    {
        $result = [];
        foreach ($array as $k => $v) {
            $result[] = $k;
            $result[] = $v;
        }

        return $result;
    }
}
