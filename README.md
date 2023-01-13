# Architect

A postgres (cockroach) migration tool. This is still alpha grade software.

# Structure

Basis a config file ([sample](https://github.com/errisnil/architect/blob/main/extras/test_config.toml)) it guides one through the process of
running migrations for a database.

A default table called `schema_migrations` is created in the database configured. This
table keeps track of the migrations run so far.

# Usage

```sh
./architect --config=<path to config> -w
```

The `-w` option above is required to run in wizard mode. One can also choose to run the binary 
using the command line args directly for explicit behaviour. Run

```sh
./architect --help
```

for available options.

## IMPORTANT

Please note that the this binary generates migration files for you and it will only work with the
naming convention ( `timestamp-millis_(up|down).sql` ) of these generated files. While you can generate
these files yourself it is highly recommended that you don't. Please use the `--new` command line
option or the `--wizard` mode to generate new migrations.

# Configuration

There are two bits of configuration to keep in mind:

## The Executable's Config

### --config
path to a .toml config file detailing connection to the database migrations will be run on.

### --migdir
path to the directory where the migration files reside. Please note that this is a
parent directory. Basis the `app` option provided in the config file a sub directory 
is created which will contain all generated migration files. Defaults to `./migrations`

### --new
a new version of migration files are generated. One for up 
[timestamp]_up.sql and one for down [timestamp]_down.sql

### --upn <N>
Migrate up `N` times or till the end of the remaining migration versions whichever happens eralier.

### --up
Migrate up all remaining versions.

### --downn <N>
Migrate down `N` times or till the beginnning of migration versions whichever happens earlier.

### --down
Migrate down till the beginning of migrations. Be very sure this is what you want to run.

### --wizard
A wizard takes over and guides you through the migration experience.

### --help
Prints help info

### --version
Prints version info

There are a few other options to be able to run 
## Database Config
This is provided by a `.toml` file using the `--config` option of the executable.
### app: String
The name of the app for which this migration is being run. The 

### dbname: String
The name of the database to connect to

### host: String
The sql server host

### port: Number
The sql server port. Default: 5432

### user: String
The user name to authenticate with

### password: String
The password to authenticate with

### ssl: Boolean
Whether the connection should use tls

### sslrootcert: String
Path of the root certificate to be used in case `ssl` is true. When not provided the env 
variable `PGSSLROOTCERT`, if set, is used. If the env variable is not set either it falls back
to `~/.postgresql/root.crt`.

### connect_timeout_seconds: Number
Maximum time to wait when establishing connection with databse server. Default of 0 will make it wait indefinitely.

# Build

```sh
cargo build --release
```

# Tests

To be able to run the tests a connection config file is required. A sample is available at
[extras/test_config.toml](https://github.com/errisnil/architect/blob/main/extras/test_config.toml).
Once it's configured for a working database run

```sh
ARCHITECT_TEST_CONFIG="./extras/test_config.toml" cargo test -- --nocapture
```

The current set of tests are very basic, checking for success cases only.

## Thank You
