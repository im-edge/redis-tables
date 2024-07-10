require('LuaUtils')
-- luacheck: std lua51, globals redis cjson LuaUtils, ignore RedisTable
-- Hint: alternative would be ignore 241 - unfortunately, there is no ignore RedisTable:241

local RedisTable = {}
RedisTable.new = function(stream, tableName, checksumLength, keyProperties)
  local self = {}
  local checksumTable = tableName .. '-checksum'
  -- local stream = tableName .. '-changes'
  local maxLenStream = 500000

  function self.setPrefixedRows(prefix, rows)
    local result = {
      'status', 'Result for setPrefixedRows?!'
    }
    local seen = {}
    local currentKeys = self.listHashKeysByPrefix(prefix)
    for key, row in pairs(rows) do
      self.setRawRow(key, row)
      seen[key] = key
    end
    for _, key in pairs(self.getLostTableKeys(currentKeys, seen)) do
      self.deleteRow(key)
    end
    return result
  end

  function self.setRawRow(key, row)
      local action = self.setRow(key, string.sub(row, 1, checksumLength), string.sub(row, checksumLength + 1))
      if action == nil then
        action = 'unchanged'
      end

      return {'status', action}
  end

  function self.setRow(key, checksum, row)
    local currentChecksum = redis.call('HGET', checksumTable, key)
    local action
    local value

    if currentChecksum == false then
      action = 'create'
      value = row
    elseif currentChecksum ~= checksum then
      local oldValues = cjson.decode(redis.call('HGET', tableName, key))
      local newValues = cjson.decode(row)
      local diff = LuaUtils.dictDiff(oldValues, newValues)
      action = 'update'
      for _, k in pairs(cjson.decode(keyProperties)) do
        -- redis.call('ECHO', 'key property: ' .. k)
        if newValues[k] == nil then
          diff[k] = cjson.null
        else
          diff[k] = newValues[k]
        end
      end
      value = cjson.encode(diff)
      -- redis.call('ECHO', 'NEW ROW ' .. value)
    else
      return nil
    end

    redis.call('HSET', tableName, key, row)
    redis.call('HSET', checksumTable, key, checksum)
    redis.call(
      'XADD', stream,
      'MAXLEN', '~', maxLenStream,
      '*',
      -- Data:
      'action',   action,
      'table', tableName,
      'key',      key,
      'keyProperties', keyProperties,
      'checksum', checksum,
      'value',    value
    )

    return action
  end

  function self.hScan(match)
    local cursor = '0'
    local result = {}
    local partialResult
    repeat
      partialResult = redis.call('HSCAN', tableName, cursor, 'MATCH', match, 'COUNT', 10000);
      cursor = partialResult[1];
      for key, value in pairs(LuaUtils.dict(partialResult[2])) do
        result[key] = value
        -- redis.call('ECHO', 'key is ' .. key)
      end
    until cursor == '0'

    return result
  end

  function self.getTableKeys(table)
    local keys = {}
    for k, _ in pairs(table) do
      keys[k] = k
    end

    return keys
  end

  function self.getLostTableKeys(t1, t2)
    local keys = {}
    for k, _ in pairs(t1) do
      if t2[k] == nil then
        keys[#keys + 1] = k
      end
    end

    return keys
  end

  function self.listHashKeysByPrefix(prefix)
    return self.getTableKeys(self.hScan(prefix .. '*'))
  end

  function self.getStreamPosition()
    local lastEntry = redis.call('XREVRANGE', stream, '+', '-', 'COUNT', '1')
    if #lastEntry == 0 then
      return nil
    end

    return lastEntry[1][1]
  end

  function self.getTableWithStreamPosition()
    local position = self.getStreamPosition()
    if position == nil then
      return nil
    end

    return { position, redis.call('HGETALL', tableName) }
  end

  function self.deleteRow(key)
    local row = redis.call('HGET', tableName, key)
    redis.call('HDEL', tableName, key)
    redis.call('HDEL', checksumTable, key)

    if row == nil then
      redis.call(
        'XADD', stream,
        'MAXLEN', '~', maxLenStream,
        '*',
        -- Data:
        'action',   'delete',
        'table', tableName,
        'key',      key,
        'keyProperties', keyProperties
      )
    else
      redis.call(
        'XADD', stream,
        'MAXLEN', '~', maxLenStream,
        '*',
        -- Data:
        'action',   'delete',
        'table', tableName,
        'key',      key,
        'keyProperties', keyProperties,
        'value',    row
      )
    end
  end

  return self
end
