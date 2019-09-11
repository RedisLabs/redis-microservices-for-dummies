local temp_set_key = KEYS[1]
local books_return_stream_key = KEYS[2]

-- Publish books in temp_set on a stream (for ShelvingService)
local books_to_return = redis.call('SMEMBERS', temp_set_key)
if #books_to_return > 0 then
	redis.call('XADD', books_return_stream_key, '*', 'book_ids', table.concat(books_to_return, ','))
end
