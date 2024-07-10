-- luacheck: std lua51, globals cjson, ignore LuaUtils
local LuaUtils = {}

LuaUtils.dict = function(tbl)
  local max = #tbl
  local result = {}
  local i = 1
  if max == 0 then
    return {}
  end
  repeat
    result[tbl[i]] = tbl[i + 1]
    i = i + 2
  until i > max

  return result
end

LuaUtils.dictDiff = function (left, right)
  local mod = {}
  -- redis.call('ECHO', 'LEFT: ' .. cjson.encode(left))
  -- redis.call('ECHO', 'RIGHT: ' .. cjson.encode(right))
  for k, v in pairs(right) do
    if v ~= left[k] then
      mod[k] = v
    end
  end
  for _, k in pairs(LuaUtils.getLostTableKeys(left, right)) do
    mod[k] = cjson.null
  end

  return mod
end

LuaUtils.getLostTableKeys = function(left, right)
  local rightKeys = {}
  local lostKeys = {}
  for k, _ in pairs(right) do
    rightKeys[k] = true
  end
  for k, _ in pairs(left) do
    if rightKeys[k] ~= true then
      table.insert(lostKeys, k)
    end
  end

  return lostKeys
end