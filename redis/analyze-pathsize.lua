local sep = ":";
local toAnalyze = KEYS[1]
local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]

local resourceCount = 0
local resourcesSize = 0

local function analyzeChildrenAndItself(path)
    local pathAbs = resourcesPrefix .. path
    if redis.call('exists',pathAbs) == 1 then
      local res_size = redis.call('hget', pathAbs, 'resource')
      resourceCount = resourceCount + 1
      resourcesSize = resourcesSize + string.len(res_size)
    elseif redis.call('exists',pathAbs) == 1 then
      local members = redis.call('zrangebyscore',pathAbs,'-inf','+inf')
      for key,value in pairs(members) do
        local pathToAnalyze = path..":"..value
        analyzeChildrenAndItself(pathToAnalyze)
      end
    else
      redis.log(redis.LOG_WARNING, "can't analyze resource from type: "..path)
    end
end

local function round(num, dp)
  return string.format("%."..(dp or 0).."f", num)
end

local function toHumanReadable(size)
    if size >= 1048576 then
      local mbs = round(size / 1048576, 2)
      return mbs.." MB"
    elseif size >= 1024 then
      local kbs = round(size / 1024, 2)
      return kbs.." KB"
    else
     return size.." Bytes"
    end
end

if "/" == toAnalyze then
    toAnalyze = ""
end

analyzeChildrenAndItself(toAnalyze)

return "Found "..resourceCount.." resources with total size of "..toHumanReadable(resourcesSize)