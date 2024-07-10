require('RedisTable')

-- luacheck: std lua51, globals redis RedisTable KEYS ARGV
local stream = KEYS[1]
local tableName = KEYS[2]
return RedisTable.new(stream, tableName).getTableWithStreamPosition()
