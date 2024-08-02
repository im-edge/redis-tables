<?php

namespace IMEdge\RedisTables;

use Amp\Redis\RedisClient;
use Exception;
use gipfl\Json\JsonString;
use IMEdge\Inventory\CentralInventory;
use IMEdge\Inventory\InventoryAction;
use IMEdge\Inventory\InventoryActionType;
use IMEdge\RedisUtils\RedisResult;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\UuidInterface;
use Revolt\EventLoop;

use function Amp\Redis\createRedisClient;

final class RedisTableSubscriber
{
    protected const STREAM_SUFFIX = '-changes';
    protected const NAME = 'IMEdge/RedisTableSubscriber';
    protected const DELAY_ON_ERROR = 15;

    protected RedisClient $redis;
    protected ?CentralInventory $inventory = null;
    protected array $tablePositions = [];
    protected ?array $xReadParams = null;
    protected bool $stopping = false;

    public function __construct(
        protected string $redisSocket,
        protected readonly UuidInterface $dataNodeUuid,
        protected readonly LoggerInterface $logger,
    ) {
        $this->logger->notice('Launching ' . self::NAME);
        $this->redis = createRedisClient('unix://' . $redisSocket);
        $this->redis->execute('CLIENT', 'SETNAME', self::NAME);
        $this->logger->notice(self::NAME . ' is ready');
    }

    public function setStreamPosition(string $table, string $position): void
    {
        $this->logger->notice("GOT Stream Position: $table: $position");
        $this->tablePositions[$table] = $position;
        $blockMs = 1000;
        $maxCount = 1000;
        $this->xReadParams = array_merge([
            'XREAD',
            'COUNT',
            (string) $maxCount,
            'BLOCK',
            (string) $blockMs,
            'STREAMS',
        ], array_map(fn ($table) => $table . self::STREAM_SUFFIX, array_keys($this->tablePositions)));
    }

    protected function readStreams(): void
    {
        if ($this->inventory === null || $this->xReadParams === null) {
            return;
        }

        $params = array_merge($this->xReadParams, array_values($this->tablePositions));
        try {
            $streams = $this->redis->execute(...$params);
            $this->processStreamResults($streams);
        } catch (Exception $e) {
            if ($this->stopping) {
                return;
            }
            $this->logger->error(sprintf(
                'Reading next Redis/ValKey stream batch failed, continuing in %ds: %s',
                self::DELAY_ON_ERROR,
                $e->getMessage()
            ));
            EventLoop::delay(self::DELAY_ON_ERROR, $this->readStreams(...));
        }
    }

    protected function processStreamResults($results): void
    {
        if (empty($results) || $this->inventory === null) {
            EventLoop::queue($this->readStreams(...));
            return;
        }
        // $this->logger->notice('GOT A STREAM RESULT: ' . count($result));
        $actions = [];
        foreach ($results as $row) {
            try {
                $table = $row[0]; // snmp_agent-changes
                if (str_ends_with($table, self::STREAM_SUFFIX)) {
                    $table = substr($table, 0, -strlen(self::STREAM_SUFFIX));
                } else {
                    throw new Exception(sprintf('"%s" is not a valid stream result table', $table));
                }
                foreach ($row[1] as $entry) {
                    $this->tablePositions[$table] = $entry[0];
                    $rawData = RedisResult::toHash($entry[1]);
                    $values = JsonString::decode($rawData->value);
                    $keyProperties = JsonString::decode($rawData->keyProperties);
                    foreach ($values as &$value) { // Fix for erroneous serialization
                        if (is_object($value) && isset($value->oid)) {
                            $value = $value->oid;
                        }
                    }
                    unset($value);
                    $action = new InventoryAction(
                        $this->dataNodeUuid,
                        $table,
                        $entry[0],
                        InventoryActionType::from($rawData->action),
                        $rawData->key,
                        $rawData->checksum ?? null,
                        $keyProperties,
                        (array) $values,
                    );
                    $actions[] = $action;
                }
                // print_r($actions);
                // echo "ACTIONS HERE\n\n";
            } catch (\Throwable $e) {
                $this->logger->error($e->getMessage() . $e->getFile() . $e->getLine());
            }
        }
        if (!empty($actions)) {
            try {
                $this->inventory->shipBulkActions($actions);
            } catch (\Throwable $e) {
                $this->logger->error(self::NAME . ' processing: ' . $e->getMessage() . $e->getFile() . $e->getLine());
            }
        }
        EventLoop::queue($this->readStreams(...));
    }

    public function setCentralInventory(CentralInventory $inventory): void
    {
        $this->inventory = $inventory;
        $this->logger->notice(self::NAME . ' got an inventory');
        EventLoop::queue($this->readStreams(...));
    }

    public function stop(): void
    {
        $this->stopping = true;
        $this->inventory = null;
    }
}
