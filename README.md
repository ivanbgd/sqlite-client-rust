[![progress-banner](https://backend.codecrafters.io/progress/sqlite/b3b12e89-a536-4604-be71-ddd7dab3056e)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own SQLite" Challenge](https://codecrafters.io/challenges/sqlite).

In this challenge, you'll build a barebones SQLite implementation that supports
basic SQL queries like `SELECT`. Along the way we'll learn about
[SQLite's file format](https://www.sqlite.org/fileformat.html), how indexed data
is
[stored in B-trees](https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/)
and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your SQLite implementation is in `src/main.rs`. Study and
uncomment the relevant code, and push your changes to pass the first stage:

```sh
git commit -am "pass 1st stage" # any msg
git push origin master
```

Time to move on to the next stage!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `cargo (1.82)` installed locally
2. Run `./your_program.sh` to run your program, which is implemented in
   `src/main.rs`. This command compiles your Rust project, so it might be slow
   the first time you run it. Subsequent runs will be fast.
3. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.

# Sample Databases

To make it easy to test queries locally, we've added a sample database in the
root of this repository: `sample.db`.

This contains two tables: `apples` & `oranges`. You can use this to test your
implementation for the first 6 stages.

You can explore this database by running queries against it like this:

```sh
$ sqlite3 sample.db "select id, name from apples"
1|Granny Smith
2|Fuji
3|Honeycrisp
4|Golden Delicious
```

There are two other databases that you can use:

1. `superheroes.db`:
    - This is a small version of the test database used in the table-scan stage.
    - It contains one table: `superheroes`.
    - It is ~1MB in size.
2. `companies.db`:
    - This is a small version of the test database used in the index-scan stage.
    - It contains one table: `companies`, and one index: `idx_companies_country`
    - It is ~7MB in size.

These aren't included in the repository because they're large in size. You can
download them by running this script:

```sh
./download_sample_databases.sh
```

If the script doesn't work for some reason, you can download the databases
directly from
[codecrafters-io/sample-sqlite-databases](https://github.com/codecrafters-io/sample-sqlite-databases).

# Running the Program

The program works from the command line, and supports the so-called dot-commands (CLI commands) as well as
SQL commands.

Supported SQL commands can be supplied in lower, upper or mixed-case, i.e., they are case-insensitive.  
The same is true of the supplied table and column names.

Values in the `WHERE` clause are case-sensitive.

Supported dot-commands are also case-insensitive.

## CLI Commands (dot-commands)

- To emulate `$ sqlite3 sample.db .dbinfo`:

```shell
$ ./your_program.sh sample.db .dbinfo
database page size: 4096
number of pages: 4
number of tables: 3
text encoding: utf-8
```

- To emulate `$ sqlite3 sample.db .tables`:

```shell
$ ./your_program.sh sample.db .tables
apples oranges
```

- To emulate `$ sqlite3 companies.db .index` or `$ sqlite3 companies.db .indexes`:

```shell
$ ./your_program.sh companies.db .index
$ ./your_program.sh companies.db .indexes
idx_companies_country
```

## SQL Commands

### Count number of rows in a table

```shell
$ ./your_program.sh sample.db "SELECT COUNT(*) FROM apples"
4
```

### Select from a single column

```shell
$ ./your_program.sh sample.db "SELECT name FROM apples"
Granny Smith
Fuji
Honeycrisp
Golden Delicious
```

Additionally, `LIMIT` is supported.

```shell
$ ./your_program.sh sample.db "SELECT name FROM apples LIMIT 3"
Granny Smith
Fuji
Honeycrisp
```

### Select from multiple columns

```shell
$ ./your_program.sh sample.db "SELECT name, color FROM apples"
Granny Smith|Light Green
Fuji|Red
Honeycrisp|Blush Red
Golden Delicious|Yellow
```

Additionally, `LIMIT` is supported.

```shell
$ ./your_program.sh sample.db "SELECT id, name, color FROM apples LIMIT 10"
1|Granny Smith|Light Green
2|Fuji|Red
3|Honeycrisp|Blush Red
4|Golden Delicious|Yellow
```

### Select Using Where

```shell
$ ./your_program.sh sample.db "SELECT id, name, color FROM apples WHERE color = 'Blush Red'"
3|Honeycrisp|Blush Red
```

Additionally, `LIMIT` is supported.

```shell
$ ./your_program.sh sample.db "SELECT id, name, color FROM apples WHERE color = 'Blush Red' LIMIT 1"
3|Honeycrisp|Blush Red
```

### Large Databases and Tables

Large tables, spanning multiple pages, are supported.

```shell
$ ./your_program.sh superheroes.db "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"
297|Stealth (New Earth)
790|Tobias Whale (New Earth)
1085|Felicity (New Earth)
2729|Thrust (New Earth)
3289|Angora Lapin (New Earth)
3913|Matris Ater Clementia (New Earth)
```

### Indexes

Indexes are also supported.

Rather than reading all rows in a table and then filtering in-memory,
we use an index to perform a more intelligent search.

To test whether our implementation actually uses an index, their tester uses a database that is ~1GB in size
and expects our program to return query results in less than 3 seconds.

The test database contains a `companies` table with an index named `idx_companies_country` on the `country` column.

We can download a small version of the `companies` database to test locally.

```shell
$ ./your_program.sh companies.db "SELECT id, name FROM companies WHERE country = 'eritrea'"
121311|unilink s.c.
2102438|orange asmara it solutions
5729848|zara mining share company
6634629|asmara rental
```

We can assume that all queries run by the tester will include `country` in the `WHERE` clause,
so they can be served by the index.
The tester will run multiple randomized queries and expect all of them to return results in under 3 seconds.
