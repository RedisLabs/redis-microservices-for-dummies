import argparse, signal, asyncio, aioredis
from services.shelving_service import ShelvingService
from services.lending_service import LendingService, BOOKS_FOR_SHELVING_STREAM_KEY

# Main event loop
loop = asyncio.get_event_loop()

# Configuration
LENDING_REQUESTS_STREAM_KEY = "lending_requests_event_stream"
BOOK_RETURN_REQUESTS_STREAM_KEY = "book_return_requests_event_stream"


async def main(action, user, books, address, db, password):
	pool = await aioredis.create_redis_pool(address, db=db, password=password, 
		minsize=4, maxsize=10, loop=loop, encoding='utf8')

	# Choose the target stream based on `action`
	stream_key = None
	if action == 'request':
		stream_key = LENDING_REQUESTS_STREAM_KEY
	elif action == 'return':
		stream_key = BOOK_RETURN_REQUESTS_STREAM_KEY
	else:
		print("Unexpected action")
		exit(1)

	# Send the request
	await pool.xadd(stream_key, {'user_id': user, 'book_ids': ','.join(books)})
	print("OK")

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='CLI tool to get and return books.')
	parser.add_argument('action', choices=['request', 'return'], type=str,
       	help='action to perform, either `request` or `return`')
	parser.add_argument('name', metavar='username', type=str,
       	help='name identifying the user')
	parser.add_argument('books', metavar='book', type=str, nargs='+',
       	help='names identifying a book')
	parser.add_argument('-a', '--address', type=str, default="redis://localhost:6379",
		help='redis address (or unix socket path) defaults to `redis://localhost:6379`')
	parser.add_argument('--db', type=int, default=0,
		help='redis database to use, defaults to 0')
	parser.add_argument('--password', type=str, default=None,
		help='redis password')
	args = parser.parse_args()

	loop.run_until_complete(main(action=args.action, user=args.name, books=args.books, 
		address=args.address, db=args.db, password=args.password))
		

