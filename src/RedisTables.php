<?php

namespace IMEdge\RedisTables;

use Amp\Redis\RedisClient;
use IMEdge\Json\JsonString;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;
use Psr\Log\LoggerInterface;
use RuntimeException;
use stdClass;

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

    /**
     * @return ?array{0: string, 1: array<string, mixed>}
     */
    public function getTable(string $table): ?array
    {
        $result = $this->lua->runScript('getTable', [$this->streamName, $table]);
        $this->logger->notice(var_export($result, true));
        if ($result === null) {
            return null;
        }
        if (is_array($result)) {
            $result[1] = array_map(JsonString::decode(...), RedisResult::toArray($result[1]));
            return $result;
        }

        throw new RuntimeException('RedisTables::getTable() got no array');
    }

    public function setTableForDevice(
        string $table,
        string $devicePrefix,
        array $keyProperties,
        array $tables
    ): string {
        $tables = array_map(self::createTableEntry(...), $tables);

        return RedisResult::toHash($this->lua->runScript('setTable', [
            $this->streamName,
            $table,
            40, // strlen($checksum)
            $devicePrefix,
            JsonString::encode($keyProperties),
        ], self::arrayToLuaTable($tables)))->status
            ?? throw new RuntimeException('Got no status for ::setTableForDevice()');
    }

    /**
     * @param string[] $keyProperties
     */
    public function setTableEntry(
        string $table,
        string $key,
        array $keyProperties,
        mixed $data
    ): string {
        return RedisResult::toHash($this->lua->runScript('setTableEntry', [
            $this->streamName,
            $table,
            40, // strlen($checksum)
            $key,
            JsonString::encode($keyProperties)
        ], [
            self::createTableEntry($data),
        ]))->status ?? throw new RuntimeException('Got no status for ::setTableEntry()');
    }

    /**
     * @param string[] $keyProperties
     */
    public function deleteTableEntry(
        string $table,
        string $key,
        array $keyProperties,
    ): bool {
        return $this->lua->runScript('deleteTableEntry', [
            $this->streamName,
            $table,
            40, // strlen($checksum)
            $key,
            JsonString::encode($keyProperties)
        ]);
    }

    protected static function createTableEntry($row): string
    {
        $json = JsonString::encode($row);
        return sha1($json) . $json;
    }

    /**
     * @param array<int|string, mixed> $array
     * @return array<mixed>
     */
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
