require('LuaUtils')
require('RedisTable')

-- luacheck: std lua51, globals redis RedisTable KEYS ARGV
local stream = KEYS[1]
local tableName = KEYS[2]
local checksumLength = KEYS[3]
local key = KEYS[4]
local keyProperties = KEYS[5]

return RedisTable.new(stream, tableName, checksumLength, keyProperties).deleteRow(key)
