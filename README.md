# Redis Microservices for Dummies
This repository contains the source code for the sample app discussed 
in the last chapter of the freely available [Redis Microservices for Dummies](https://redislabs.com) book.

## Overview
This is a sample application written in Python that models the core 
functionality of a library with an automated book request/return system.

The application is made up of two services that communicate via event streams and
it's meant to exemplify how the microservices architecture influcences data modeling
and communication. 

## Project Struture

### `services/`
Contains the implementation of two services: `LendingService` and `ShelvingService`.
Most of the implemented functionality is in `LendingService`. 
`ShelvingService` represents the main storage of the library, while `LendingService` 
represents the robotic arm tasked with fetching books from the library. We also assume
that `LendingService` has a small storage for frequently-requested books.


### `lua/`
Contains the [Lua scripts](https://redis.io/commands/eval) that `LendingService` 
uses to perform some operations with transactional semantics (isolation, all-or-nothing).

### `main.py`
Loads the application. It's also possible to launch mutiple instances in parallel (i.e., supports horizontal scaling).
```
usage: main.py [-h] [-f] [-a ADDRESS] [--db DB] [--password PASSWORD] [--ssl]
               unique_name

LendingService sample implementation.

positional arguments:
  unique_name           unique name for this instance

optional arguments:
  -h, --help            show this help message and exit
  -f, --force           start even if there is a lock on this instance name
  -a ADDRESS, --address ADDRESS
                        redis address (or unix socket path) defaults to
                        `redis://localhost:6379`
  --db DB               redis database to use, defaults to 0
  --password PASSWORD   redis password
  --ssl                 use ssl
  ```

### `get_books.py`
Allows you to request and return books. The result of each request will be logged by `main.py`.
```
usage: get_books.py [-h] [-a ADDRESS] [--db DB] [--password PASSWORD] [--ssl]
                    {request,return} username book [book ...]

CLI tool to get and return books.

positional arguments:
  {request,return}      action to perform, either `request` or `return`
  username              name identifying the user
  book                  names identifying a book

optional arguments:
  -h, --help            show this help message and exit
  -a ADDRESS, --address ADDRESS
                        redis address (or unix socket path) defaults to
                        `redis://localhost:6379`
  --db DB               redis database to use, defaults to 0
  --password PASSWORD   redis password
  --ssl                 use ssl
  ```

## Usage
For convenience we assume that the library has a unique copy of every possible book. 
This means that all requests for new books will always succeed.
Requesting a book that is already being lent to another user will not succeed.
Users also have a limit of max 5 books they and be lent at any given time.


### Usage example

First make sure to install all the dependences:

`pip install -r requirements.txt`

Then launch the main process:

`python main.py worker1`

Finally, in another terminal:

`python get_books.py request user1 alice-in-wonderland geb invisible-citites`

`python get_books.py return user1 invisible-cities`

`python get_books.py request user2 geb invisible-cities selfish-gene`




