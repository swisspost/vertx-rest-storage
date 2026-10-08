-- Cluster-mode variant of del.lua, used only when redisClusterPartitioningEnabled is true and the
-- target path has a partition tag (see org.swisspush.reststorage.redis.RedisStorage#delete). Kept as
-- its own file - rather than folded back into del.lua - so the non-cluster script can keep evolving
-- independently and never has to account for cluster-only concerns.
--
-- The "REMOVE THE ORPHAN PARENTS" walk below climbs from the deleted path back up towards the root,
-- zcount'ing/zrem'ing each ancestor's collectionsPrefix key. The topmost ancestor it ever reaches is
-- always the empty string (KEYS[1] always starts with a leading separator), i.e. the single, global,
-- untagged collectionsPrefix key (no path suffix) that is inherently shared across every partition tag.
-- That key can never itself be part of the same hash tag as KEYS[1], so touching it here would be a
-- Redis Cluster cross-slot access. This variant simply skips every access guarded by "path == ''"
-- below; every other ancestor level always already contains the {tag} hash tag substring (embedded
-- right after the leading separator), so it is safe to access as-is.
local sep = ":";
local toDelete = KEYS[1]


-- Important: This Script here is includes in cleanup.lua. The ARGV-Array is used again in this including script
-- Remember to harmonize the cleanup.lua ARGV parameters with ordering, format and purpose here in THIS script
-- Take care when using "return" in this script, see https://github.com/swisspush/vertx-rest-storage/issues/83
local resourcesPrefix = ARGV[1]
local collectionsPrefix = ARGV[2]
local deltaResourcesPrefix = ARGV[3]
local deltaEtagsPrefix = ARGV[4]
local expirableSet = ARGV[5]
local minscore = tonumber(ARGV[6])
local maxscore = tonumber(ARGV[7])
local confirmCollectionDelete = ARGV[8]
local deleteRecursive = ARGV[9]
local lockPrefix = ARGV[10]
local lockOwner = ARGV[11]
local lockMode = ARGV[12]
local lockExpire = ARGV[13]

local function deleteChildrenAndItself(path)
    if redis.call('exists',resourcesPrefix..path) == 1 then
      redis.log(redis.LOG_NOTICE, "del: "..resourcesPrefix..path)
      redis.call('zrem', expirableSet, resourcesPrefix..path)
      redis.call('del', resourcesPrefix..path)
      redis.call('del', deltaResourcesPrefix..path)
      redis.call('del', deltaEtagsPrefix..path)
      redis.call('del', lockPrefix..path)
    elseif redis.call('exists',collectionsPrefix..path) == 1 then
      local members = redis.call('zrangebyscore',collectionsPrefix..path,minscore,maxscore)
      for key,value in pairs(members) do
        local pathToDelete = path..":"..value
        deleteChildrenAndItself(pathToDelete)
      end
      redis.call('del', collectionsPrefix..path)
    else
      redis.log(redis.LOG_WARNING, "can't delete resource: "..path)
    end
end

local setLockIfClaimed = function()
    if lockOwner ~= nil and lockOwner ~= '' then
        redis.call('hmset', lockPrefix..KEYS[1], 'owner', lockOwner, 'mode', lockMode)
        redis.call('pexpireat',lockPrefix..KEYS[1], lockExpire)
    end
end

local scriptState = "notFound"

local isResource = redis.call('exists',resourcesPrefix..toDelete)
local isCollection = redis.call('exists',collectionsPrefix..toDelete)

if confirmCollectionDelete == "true" and deleteRecursive == "false" and isCollection == 1 then
    redis.log(redis.LOG_NOTICE, "delete on collection requires recursive=true parameter")
    return "notEmpty"
end

if isResource == 1 or isCollection == 1 then

  if isResource and  redis.call('exists',lockPrefix..toDelete) == 1 then
    local result = redis.call('hmget',lockPrefix..KEYS[1],'owner','mode')
    if result[1] ~= lockOwner then
        return result[2]
    end
  end

  local score = tonumber(redis.call('zscore',expirableSet,resourcesPrefix..toDelete))  
  local expired = 0
  if score ~= nil and minscore > score then
    redis.log(redis.LOG_NOTICE, "expired: "..resourcesPrefix..toDelete)
    expired = 1
  end
  
  if expired == 0 then
  
    -- REMOVE THE CHILDREN
    deleteChildrenAndItself(toDelete)
    
    if redis.call('zcount', collectionsPrefix..toDelete,minscore,maxscore) == 0 then
      
      -- REMOVE THE ORPHAN PARENTS
      local path = toDelete..sep
      local nodes = {path:match((path:gsub("[^"..sep.."]*"..sep, "([^"..sep.."]*)"..sep)))}
      local pathDepth=0
      local pathState
      local nodetable = {}
      local pathtable = {}
      for key,value in pairs(nodes) do
          if pathState == nil then
              pathState = value
          else
            pathState = pathState..sep..value
          end 
          redis.log(redis.LOG_NOTICE, "add path: "..pathDepth.." "..pathState)
          pathtable[pathDepth] = pathState
          nodetable[pathDepth] = value
          pathDepth = pathDepth + 1
      end
      
      table.remove(pathtable,pathDepth)
      
      local orphanParents = 1
      -- pathtable[pathDepth-2] == '' is the untagged root ancestor (see header): treated as "no known
      -- sibling count", which keeps orphanParents at its default (1) instead of reading a cross-slot key.
      local parentCount = 0
      if pathtable[pathDepth-2] ~= '' then
        parentCount = redis.call('zcount', collectionsPrefix..pathtable[pathDepth-2],minscore,maxscore)
      end
      redis.log(redis.LOG_NOTICE, "parentCount: "..parentCount)
      redis.log(redis.LOG_NOTICE, "pathDepth: "..pathDepth)
      if pathDepth > 1 and parentCount > 1 then
        orphanParents = 0
      end
      
      redis.log(redis.LOG_NOTICE, "orphanParents: "..orphanParents)
      
      local directParent = 1
      local stopDel = 0
      for pathDepthState = pathDepth, 2, -1 do
        redis.log(redis.LOG_NOTICE, "pathState: "..pathtable[pathDepthState-2].." "..pathDepthState)
        local ancestorKey = pathtable[pathDepthState-2]
        if orphanParents == 1 and stopDel == 0 and ancestorKey ~= '' then
            if redis.call('zcount', collectionsPrefix..ancestorKey,0,maxscore) > 1 then
              stopDel = 1
            end
            redis.log(redis.LOG_NOTICE, "zrem: "..collectionsPrefix..ancestorKey.." "..nodetable[pathDepthState-1])
            redis.call('zrem', collectionsPrefix..ancestorKey, nodetable[pathDepthState-1])
        end
        if directParent == 1 then
          local directAncestorKey = pathtable[pathDepth-2]
          if directAncestorKey ~= '' then
            redis.log(redis.LOG_NOTICE, "remove direct parent")
            redis.log(redis.LOG_NOTICE, "zrem: "..collectionsPrefix..directAncestorKey.." "..nodetable[pathDepthState-1])
            redis.call('zrem', collectionsPrefix..directAncestorKey, nodetable[pathDepthState-1])
          end
          directParent = 0
        end
      end
    end

    if isResource then
        setLockIfClaimed()
    end

    scriptState = "deleted"
  end
else
  redis.log(redis.LOG_WARNING, "resource "..toDelete.." not present, will remove possible entry in expirableSet anyway")
  -- remove orphan entry in the expirableSet anyway (if there is actually one)
  redis.call('zrem', expirableSet, resourcesPrefix..toDelete)
end

return scriptState
