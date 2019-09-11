local user_id = ARGV[1]
local lent_books_key = KEYS[1]
local temp_set_key = KEYS[2]
local user_counts_key = KEYS[3]

-- For each book, update `lent_books` if the user_id matches,
-- otherwise just remove it from the set.
for _, book_id in ipairs(redis.call('SMEMBERS', temp_set_key)) do
	if user_id == redis.call('HGET', lent_books_key, book_id) then
		redis.call('HDEL', lent_books_key, book_id)
	else
		redis.call('SREM', temp_set_key, book_id)
	end
end

-- All the books that remain in `temp_set` are the books that 
-- were succesfully returned. We now need to update the count.
local returned_count = redis.call('SCARD', temp_set_key)
if returned_count > 0 then
	redis.call('INCRBY', user_counts_key, -returned_count)
end
