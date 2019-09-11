import asyncio
import aioredis
loop = asyncio.get_event_loop()


## -- SERVICE STATE (stored in Redis) --
# AUTOMATED_BOOK_STORAGE_KEY is a Redis Set that contains BookIDs.
# Books in the automated storage can be returned immediately to
# the user. The book storage has limited space so when full books
# must be handed off to BookShelvingService.
AUTOMATED_BOOK_STORAGE_KEY = "automated_book_storage"

# LENT_BOOKS_KEY is a Redis Hash that maps each lent BookID to the 
# UserID that presently has it. 
LENT_BOOKS_KEY = "lent_books"

# Each user has a key that counts how many books they currently possess
BOOK_COUNTS_KEY_TEMPLATE = "user:{user_id}:count"

# While fullfilling a request, the service will progressively
# reserve books by moving their IDs into a temporary Redis set
# that is unique to each request.
REQUEST_RESERVED_BOOKS_KEY_TEMPLATE = "request:{request_id}:temp"


##  -- SERVICE CONFIGURATION --
# Represents the maximum number of books that the automated
# storage can keep.
AUTOMATED_BOOK_STORAGE_CAPACITY = 20

# Maximum number of books a single user can have at the same time.
# Ulterior requests for books will be denied once the limit is reached.
MAX_LENT_BOOKS_PER_USER = 5

# Names of required streams and consumer groups
LENDING_SERVICE_CONSUMER_GROUP = 'lending_service'
LENDING_REQUESTS_STREAM_KEY = "lending_requests_event_stream"
BOOK_RETURN_REQUESTS_STREAM_KEY = "book_return_requests_event_stream"
BOOKS_FOR_SHELVING_STREAM_KEY = "books_for_shelving_event_stream"

# Global variable containing the SHA1 digest of `return_books.lua`
# It gets populated during startup by `main()`.
REFILL_AUTOMATED_STORAGE_LUA = None
BOOKS_TO_STREAM_LUA = None
APPLY_BOOK_RETURN_LUA = None


class LendingService:
	def __init__(self, pool, instance_name, shelving_service):
		self.pool = pool
		self.instance_name = instance_name
		self.shutting_down = False
		self.shelving_service = shelving_service


	async def launch_service(self):
		# Ensure Redis has a consumer group defined for each relevant stream
		try:
			await self.pool.execute("XGROUP", "CREATE", LENDING_REQUESTS_STREAM_KEY, LENDING_SERVICE_CONSUMER_GROUP, "$", "MKSTREAM")
		except aioredis.errors.ReplyError as e:
			assert e.args[0].startswith("BUSYGROUP")
		try:
			await self.pool.execute("XGROUP", "CREATE", BOOK_RETURN_REQUESTS_STREAM_KEY, LENDING_SERVICE_CONSUMER_GROUP, "$", "MKSTREAM")
		except aioredis.errors.ReplyError as e:
			assert e.args[0].startswith("BUSYGROUP")

		# Ensure Redis has the required Lua scripts
		global REFILL_AUTOMATED_STORAGE_LUA, BOOKS_TO_STREAM_LUA, APPLY_BOOK_RETURN_LUA
		REFILL_AUTOMATED_STORAGE_LUA = await self.pool.script_load(open('lua/refill_automated_storage.lua', 'r').read())
		BOOKS_TO_STREAM_LUA = await self.pool.script_load(open('lua/books_to_stream.lua', 'r').read())
		APPLY_BOOK_RETURN_LUA = await self.pool.script_load(open('lua/apply_book_return.lua', 'r').read())

		# First we retrieve any potential pending message
		events = await self.pool.xread_group(LENDING_SERVICE_CONSUMER_GROUP, self.instance_name,
			[LENDING_REQUESTS_STREAM_KEY, BOOK_RETURN_REQUESTS_STREAM_KEY], latest_ids=["0", "0"])
		if len(events) > 0:
			print("[WARN] Found claimed events that need processing, resuming...")

		# This is the main loop
		print("Ready to process events...")
		with await self.pool as conn:
			while not self.shutting_down:
				tasks = []
				for stream_name, event_id, message in events:
					if stream_name == LENDING_REQUESTS_STREAM_KEY:
						tasks.append(self.process_lending_request(event_id, message))
					elif stream_name == BOOK_RETURN_REQUESTS_STREAM_KEY:
						tasks.append(self.process_returned_books_request(event_id, message))
				await asyncio.gather(*tasks)

				# Gather new events to process (batch size = 10)
				events = await conn.xread_group(LENDING_SERVICE_CONSUMER_GROUP, self.instance_name,
					[LENDING_REQUESTS_STREAM_KEY, BOOK_RETURN_REQUESTS_STREAM_KEY], timeout=10000, count=10, latest_ids=[">", ">"])


	async def process_lending_request(self, request_id, request):
		user_id = request['user_id']
		requested_books = set(request['book_ids'].split(','))
		user_book_counts_key = BOOK_COUNTS_KEY_TEMPLATE.format(user_id=user_id)
		request_reserved_books_key = REQUEST_RESERVED_BOOKS_KEY_TEMPLATE.format(request_id=request_id)

		## -- PROCESS REQUEST --

		# See if Redis contains already a partial set of reserved books.
		# This can happen in the case of a crash. In such case, we fetch
		# the set to resume from where we left off.
		books_found = await self.pool.smembers(request_reserved_books_key)
		if len(books_found) > 0:
			print("[WARN] Found partially-processed transaction, resuming.")

		for book_id in requested_books:
			# Was the book already reserved before a crash?
			if book_id in books_found:
				continue

			# Is the book lent already?
			if await self.pool.hexists(LENT_BOOKS_KEY, book_id):
				continue

			# Try to get the book from automated storage
			if await self.pool.smove(AUTOMATED_BOOK_STORAGE_KEY, request_reserved_books_key, book_id):
				books_found.append(book_id)
				continue

			# Try to get the book from ShelvingService
			# get_book() is idempotent, so it's ok to call it again
			# in case of a crash. 
			if await self.shelving_service.get_book(book_id, request_id):
				await self.pool.sadd(request_reserved_books_key, book_id)
				books_found.append(book_id)
				continue

		# Requests for which we can't find any book get denied.
		# This is an arbitrary choice, but it makes more sense than
		# to accept a request and then give out 0 books.
		if len(books_found) == 0:
			await self.pool.xack(LENDING_REQUESTS_STREAM_KEY, LENDING_SERVICE_CONSUMER_GROUP, request_id)
			print(f"Request: [{request_id}] by {user_id} DENIED.")
			print(f"	Cause: none of the requested books is available.\n")
			return

		# Reserving a connection for the upcoming transaction
		with await self.pool as conn:

			# The transaction can fail if another process is changing `user_book_counts_key`.
			# We retry indefinitely but know that the transaction will eventually succeed
			# because at every iteration at least 1 concurrent process will always move forward.
			# More generally, given the domain we chose, we don't expect users to perform
			# multiple requests at the same, normally. The transaction main purpose is to
			# prevent abuse where one user might try to perform multiple parallel requests 
			# with the explicit goal of going over `MAX_LENT_BOOKS_PER_USER`.
			while True:
				# We WATCH `user_book_counts_key` to perform optimistinc locking over it.
				await conn.watch(user_book_counts_key)

				books_in_hand = int((await conn.get(user_book_counts_key)) or 0)
				if (len(books_found) + books_in_hand > MAX_LENT_BOOKS_PER_USER):
					# The user is overdrafting. We deny the request and roll-back
					# all reservations.

					await conn.unwatch()
					transaction = conn.multi_exec()

					# Refill local storage
					transaction.evalsha(REFILL_AUTOMATED_STORAGE_LUA, 
						keys=[AUTOMATED_BOOK_STORAGE_KEY, request_reserved_books_key], 
						args=[AUTOMATED_BOOK_STORAGE_CAPACITY])

					# Return remaining books
					transaction.evalsha(BOOKS_TO_STREAM_LUA, 
						keys=[request_reserved_books_key, BOOKS_FOR_SHELVING_STREAM_KEY])

					# Cleanup
					transaction.unlink(request_reserved_books_key)
					transaction.xack(LENDING_REQUESTS_STREAM_KEY, LENDING_SERVICE_CONSUMER_GROUP, request_id)
					await transaction.execute()
					print(f"Request: [{request_id}] by {user_id} DENIED.")
					print(f"	Cause: too many books (user has {books_in_hand} books, requested {len(requested_books)} of which {len(books_found)} were found).\n")
				else:
					# The user has enough capacity to get all the found books.
					# All temporarily reserved books will now be committed.
					transaction = conn.multi_exec()
					transaction.incrby(user_book_counts_key, len(books_found))
					for book_id in books_found:
						transaction.hset(LENT_BOOKS_KEY, book_id, user_id)
					transaction.unlink(request_reserved_books_key)
					transaction.xack(LENDING_REQUESTS_STREAM_KEY, LENDING_SERVICE_CONSUMER_GROUP, request_id)
					try:
						await transaction.execute()
						print(f"Request: [{request_id}] by {user_id} ACCEPTED.")
						print(f"Books:")
						for i, book_id in enumerate(books_found):
							print(f"	{i + 1}) {book_id}")
						print()
					except aioredis.WatchError:
						# If the transaction failed because the watched key
						# changed in the meantime, we retry the transaction.
						continue
				break
		return

	async def process_returned_books_request(self, return_request_id, return_request):
		user_id = return_request['user_id']
		book_id_list = set(return_request['book_ids'].split(','))
		user_book_counts_key = BOOK_COUNTS_KEY_TEMPLATE.format(user_id=user_id)
		temp_set_key = f"return:{return_request_id}:temp"

		with await self.pool as conn:
			# Start the transaction and load the data
			transaction = conn.multi_exec()
			transaction.sadd(temp_set_key, *book_id_list)

			# Apply book returns (updates `lent_books` and the user's book count)
			transaction.evalsha(APPLY_BOOK_RETURN_LUA, 
				keys=[LENT_BOOKS_KEY, temp_set_key, user_book_counts_key], 
				args=[user_id])

			# Refill automated storage
			transaction.evalsha(REFILL_AUTOMATED_STORAGE_LUA, 
				keys=[AUTOMATED_BOOK_STORAGE_KEY, temp_set_key], 
				args=[AUTOMATED_BOOK_STORAGE_CAPACITY])

			# Return remaining books
			transaction.evalsha(BOOKS_TO_STREAM_LUA, 
				keys=[temp_set_key, BOOKS_FOR_SHELVING_STREAM_KEY])

			# Cleanup
			transaction.unlink(temp_set_key)
			transaction.xack(BOOK_RETURN_REQUESTS_STREAM_KEY, LENDING_SERVICE_CONSUMER_GROUP, return_request_id)
			await transaction.execute()
			print(f"Book Return [{return_request_id}] PROCESSED \n")



