import argparse, signal, asyncio, aioredis
from services.shelving_service import ShelvingService
from services.lending_service import LendingService, BOOKS_FOR_SHELVING_STREAM_KEY

# Main event loop
loop = asyncio.get_event_loop()

# Global list of running services, used by `graceful_shutdown`
RUNNING_SERVICES = None
SHUTTING_DOWN = False

# Shutdown signal handler
def graceful_shutdown():
	global SHUTTING_DOWN
	if SHUTTING_DOWN:
		print("\nForcing shutdown...")
		exit(1)
	SHUTTING_DOWN = True
	for service in RUNNING_SERVICES:
		service.shutting_down = True
	print("\nShutting down (might take up to 10s)...")

async def main(instance_name, force, address, db, password, ssl):
	pool = await aioredis.create_redis_pool(address, db=db, password=password, ssl=ssl,
		minsize=4, maxsize=10, loop=loop, encoding='utf8')

	lock_key = f"instance_lock:{instance_name}"
	if not force:
		if not await pool.setnx(lock_key, 'locked'):
			print("There might be another instance with the same name running.")
			print("Use the -f option to force launching anyway.")
			print("For the service to work correctly, each running instance must have a unique name.")
			exit(1)

	# Setup a singnal handler for graceful shutdown
	loop.add_signal_handler(signal.SIGINT, graceful_shutdown)

	# Instantiate LendingService and ShelvingService
	shelving_service = ShelvingService(pool, instance_name, BOOKS_FOR_SHELVING_STREAM_KEY)
	lending_service = LendingService(pool, instance_name, shelving_service)

	# Add services to `RUNNING_SERVICES` list to enable graceful shutdown
	global RUNNING_SERVICES
	RUNNING_SERVICES = [lending_service, shelving_service]

	# Launch all services
	await asyncio.gather(lending_service.launch_service(), shelving_service.launch_service())

	# Release the instance name lock when shutting down
	await pool.delete(lock_key)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='LendingService sample implementation.')
	parser.add_argument('name', metavar='unique_name', type=str,
       	help='unique name for this instance')
	parser.add_argument('-f', '--force', action='store_true',
	    help='start even if there is a lock on this instance name')
	parser.add_argument('-a', '--address', type=str, default="redis://localhost:6379",
		help='redis address (or unix socket path) defaults to `redis://localhost:6379`')
	parser.add_argument('--db', type=int, default=0,
		help='redis database to use, defaults to 0')
	parser.add_argument('--password', type=str, default=None,
		help='redis password')
	parser.add_argument('--ssl', action='store_true', default=None,
		help='use ssl')
	args = parser.parse_args()

	loop.run_until_complete(main(instance_name=args.name, force=args.force, 
		address=args.address, db=args.db, password=args.password, ssl=args.ssl))
		

