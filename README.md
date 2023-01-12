# Architect

A postgres (cockroach) migration tool. This is still alpha grade software.

# Structure

Basis a config file (sample: extras/test_config.toml) it guides one through the process of running migrations for a database.

A default table called `schema_migrations` is created in the database configured. This table keeps track of the migrations run so far and helps with migrating down.

# Usage

```sh
./architect --config=<path to config> -w
```

The `-w` option above is required to run in wizard mode. One can also chhose to run the binary without using the command line args directly for explicit behaviour. Run

```sh
./architect --help
```

for available options

# Tests

To be able to run the tests a connection config file is required. A sample is available at `extras/test_config.toml`. Once that is configured for a working database run

```sh
ARCHITECT`_TEST_CONFIG="./extras/test_config.toml" cargo test -- --nocapture
```

The current set of tests are very basic, checking for success cases only.

## Thank You
