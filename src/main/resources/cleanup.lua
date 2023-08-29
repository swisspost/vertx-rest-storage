local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]
local deltaResourcesPrefix = ARGV[3]
local deltaEtagsPrefix = ARGV[4]
local expirableSet = ARGV[5]
local minscore = tonumber(ARGV[6])
local maxscore = tonumber(ARGV[7])
local confirmCollectionDelete = ARGV[8]
local deleteRecursive = ARGV[9]
local now = tonumber(ARGV[10])
local bulksize = tonumber(ARGV[11])

-- Important: The ARGV-Array is used again in the included del.lua script
-- (see this funny comment with the percent sign below and Java-Method
--      org.swisspush.reststorage.redis.RedisStorage.LuaScriptState.composeLuaScript)
-- we need to initialize all parameters for del.lua here - otherwise we can have side effects
-- See open issue https://github.com/swisspush/vertx-rest-storage/issues/83
ARGV[10] = ''
ARGV[11] = ''
ARGV[12] = ''
ARGV[13] = ''

local resourcePrefixLength = string.len(resourcesPrefix)
local counter = 0
local KEYS = {}
local resourcesToClean = redis.call('zrangebyscore',expirableSet,minscore,now,'limit',0,bulksize)
for key,value in pairs(resourcesToClean) do
  redis.log(redis.LOG_NOTICE, "cleanup resource: "..value)
  KEYS[1] = string.sub(value, resourcePrefixLength+1, string.len(value))
  
  --%(delscript)
  
  counter = counter + 1
end
return counter