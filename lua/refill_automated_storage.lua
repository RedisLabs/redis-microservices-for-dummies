local max_automated_storage_size = ARGV[1]
local automated_storage_key = KEYS[1]
local temp_set_key = KEYS[2]

-- Move as many books as possible to automated storage
local free_space = max_automated_storage_size - redis.call('SCARD', automated_storage_key)
if free_space > 0 then
	local books_to_move = redis.call('SPOP', temp_set_key, free_space)
	if #books_to_move > 0 then
		redis.call('SADD', automated_storage_key, unpack(books_to_move))
	end
end

