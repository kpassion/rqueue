-- Copyright 2020 Sonu Kumar
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--       https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

local expiredValues = redis.call('ZRANGEBYSCORE', KEYS[2], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
if #expiredValues > 0 then
    for _, v in ipairs(expiredValues) do
        redis.call('RPUSH', KEYS[1], v)
    end;
    redis.call('ZREM', KEYS[2], unpack(expiredValues))
end
-- check head of the queue
local v = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if v[1] ~= nil then
    local score = tonumber(v[2])
    return score
end
return nil;