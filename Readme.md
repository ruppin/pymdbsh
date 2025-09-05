# pymdb: Python MongoDB CLI Tool

A Python command-line interface for MongoDB, inspired by SQL shells and designed for interactive database exploration, scripting, and automation.

---

## Features

- **Interactive Shell (REPL):**
  - Command history and editing.
  - Graceful exit (`exit` or `quit`).
- **Configuration Management:**
  - Store multiple connections in `~/.mongo_cli.conf`.
  - Switch between connections with `use <name>`.
  - Supports both `connection_string` and host/port authentication.
- **Query Execution:**
  - Direct MongoDB syntax: `db.collection.find({...})`
  - SQL-like syntax: `SELECT field FROM collection WHERE ... ORDER BY ... LIMIT ...` (auto-translated)
  - Pretty-printed JSON output (handles `datetime` and BSON types).
- **Variables and Aliases:**
  - Define variables: `set user_id = 123`
  - Use variables in queries: `db.users.find({"_id": "$user_id"})`
  - Define aliases: `alias get_users = db.users.find({})`
- **Piping and Redirection:**
  - Pipe output to shell commands: `db.users.find({}) | grep "Alice"`
  - Redirect output to files: `db.users.find({}) > users.json`
- **Advanced SQL-to-Mongo Translation:**
  - Supports `SELECT *` for all fields.
  - Supports `WHERE` with `=`, `!=`, `>`, `<`, `>=`, `<=`, and boolean values.
  - Supports `ORDER BY field [ASC|DESC]`.
  - Supports `LIMIT n`.

---

## SQL-to-Mongo Translation

- **Supported SQL:**
  - `SELECT * FROM collection`
  - `SELECT field1,field2 FROM collection`
  - `SELECT field FROM collection WHERE field = 'value'`
  - `SELECT * FROM collection WHERE age > 21 ORDER BY age DESC LIMIT 5`
  - Supports `AND` in WHERE clause.

**Examples:**
```
SELECT * FROM users WHERE age > 21 ORDER BY age DESC LIMIT 5
```
Translates to:
```
db.users.find({"age": {"$gt": 21}}).sort([("age", -1)]).limit(5)
```

---

## Usage

### 1. Configuration

Create `~/.mongo_cli.conf`:

```ini
[default]
host = localhost
port = 27017
database = test

[atlas]
connection_string = mongodb+srv://user:pass@cluster0.mongodb.net/mydb?retryWrites=true&w=majority
database = mydb

[variables]
user_id = 123

[aliases]
get_users = db.users.find({})
get_user_by_id = db.users.find({"_id": "$user_id"})
```

### 2. Start the CLI

```sh
python pymdbsh.py
```

### 3. Example Commands

- Show current database:
  ```
  mongo> db
  ```
- Find documents:
  ```
  mongo> db.users.find({})
  ```
- SQL-like query:
  ```
  mongo> SELECT name FROM users WHERE age >= 18 AND active != false ORDER BY name ASC LIMIT 10
  ```
- Set variable:
  ```
  mongo> set user_id = 456
  ```
- Use alias:
  ```
  mongo> get_user_by_id
  ```
- Pipe output:
  ```
  mongo> db.users.find({}) | grep "Alice"
  ```
- Redirect output:
  ```
  mongo> db.users.find({}) > users.json
  ```
- Switch connection:
  ```
  mongo> use atlas
  ```
- Exit:
  ```
  mongo> exit
  ```

---

## Requirements

- Python 3.10+
- pymongo
- prompt_toolkit
- dnspython (for SRV connection strings)
- bson (comes with pymongo)

Install dependencies:
```sh
pip install pymongo prompt_toolkit dnspython
```

---

## License