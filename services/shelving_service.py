import asyncio
import aioredis

SHELVING_SERVICE_STATE_KEY = "shelving_service_state"
SHELVING_SERVICE_CONSUMER_GROUP = 'shelving_service'

# ShelvingService represents the library subsystem that is supposed
# to put books back on the appropriate shelf.
# Shelving service has less business logic than LendingService.
# It has been implemented mainly to properly process returned books
# so that users can use LendingService and see it behave correctly.

class ShelvingService:

	def __init__(self, pool, instance_name, lending_service_returns_stream_key):
		self.pool = pool
		self.lending_service_returns_stream_key = lending_service_returns_stream_key
		self.instance_name = instance_name
		self.shutting_down = False

	# Public synchronous API. 
	# In a microservices architecture you would probably
	# access this method through an HTTP / gRPC request.
	async def get_book(self, book_id, context_id):
		# Try to get the book
		if 1 == await self.pool.hsetnx(SHELVING_SERVICE_STATE_KEY, book_id, context_id):
			return True
		else:
			# Book is taken. If it was taken from this same context, return success anyway
			return context_id == await self.pool.hget(SHELVING_SERVICE_STATE_KEY, book_id)

	# Internal APIs
	async def launch_service(self):
		# Ensure we have a consumer group
		try:
			await self.pool.execute("XGROUP", "CREATE", self.lending_service_returns_stream_key, SHELVING_SERVICE_CONSUMER_GROUP, "$", "MKSTREAM")
		except aioredis.errors.ReplyError as e:
			assert e.args[0].startswith("BUSYGROUP")

		# Get pending returns
		events = await self.pool.xread_group(SHELVING_SERVICE_CONSUMER_GROUP, self.instance_name,
			[self.lending_service_returns_stream_key], latest_ids=["0"])
		
		with await self.pool as conn:
			while not self.shutting_down:
				tasks = [self.process_return(event_id, message) for _, event_id, message in events]
				await asyncio.gather(*tasks)

				# Get more returns
				events = await conn.xread_group(SHELVING_SERVICE_CONSUMER_GROUP, self.instance_name,
					[self.lending_service_returns_stream_key], timeout=10000, latest_ids=[">"])
				
	async def process_return(self, event_id, message):
		book_list = message['book_ids'].split(',')
		transaction = self.pool.multi_exec()
		transaction.hdel(SHELVING_SERVICE_STATE_KEY, *book_list)
		transaction.xack(self.lending_service_returns_stream_key, SHELVING_SERVICE_CONSUMER_GROUP, event_id)
		await transaction.execute()