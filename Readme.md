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
- **Query Execution:**
  - Direct MongoDB syntax: `db.collection.find({...})`
  - SQL-like syntax: `SELECT field FROM collection WHERE ...` (auto-translated)
  - Pretty-printed JSON output.
- **Variables and Aliases:**
  - Define variables: `set user_id = 123`
  - Use variables in queries: `db.users.find({"_id": "$user_id"})`
  - Define aliases: `alias get_users = db.users.find({})`
- **Piping and Redirection:**
  - Pipe output to shell commands: `db.users.find({}) | grep "Alice"`
  - Redirect output to files: `db.users.find({}) > users.json`

---

## SQL-to-Mongo Translation

- **Supported SQL:**
  - `SELECT field1,field2 FROM collection`
  - `SELECT field FROM collection WHERE field = 'value'`
  - Supports `AND`, `=`, `!=`, `>`, `<`, `>=`, `<=`, and boolean values in WHERE clause.

**Examples:**
```
SELECT name,age FROM users WHERE age > 21 AND active = true
```
Translates to:
```
db.users.find({"age": {"$gt": 21}, "active": true}, {"name": 1, "age": 1})
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

[variables]
user_id = 123

[aliases]
get_users = db.users.find({})
get_user_by_id = db.users.find({"_id": "$user_id"})
```

### 2. Start the CLI

```sh
python pymdb.py
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
  mongo> SELECT name FROM users WHERE age >= 18 AND active != false
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
  mongo> use default
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

Install dependencies:
```sh
pip install pymongo prompt_toolkit
```

---