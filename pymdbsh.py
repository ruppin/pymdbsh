import sys
import os
import pymongo
import configparser
import subprocess
import shlex
import json
import re
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from bson import json_util

class MongoCLI:
    def __init__(self, config_file_path):
        self.client = None
        self.db = None
        self.variables = {}
        self.aliases = {}
        self.configs = self.load_config(config_file_path)
        self.current_conn = list(self.configs.keys())[0] if self.configs else None
        if self.current_conn:
            self.connect(self.current_conn)

    def load_config(self, path):
        configs = {}
        variables = {}
        aliases = {}
        parser = configparser.ConfigParser()
        path = os.path.expanduser(path)
        if os.path.exists(path):
            parser.read(path)
            for section in parser.sections():
                if section.lower() == "variables":
                    for k, v in parser.items(section):
                        variables[k] = v
                elif section.lower() == "aliases":
                    for k, v in parser.items(section):
                        for var_k, var_v in variables.items():
                            v = v.replace(f"${var_k}", str(var_v))
                        aliases[k] = v
                else:
                    # Support connection_string or host/port
                    conn_string = parser.get(section, 'connection_string', fallback=None)
                    if conn_string:
                        configs[section] = {
                            'connection_string': conn_string,
                            'database': parser.get(section, 'database', fallback=None)
                        }
                    else:
                        configs[section] = {
                            'host': parser.get(section, 'host', fallback='localhost'),
                            'port': parser.getint(section, 'port', fallback=27017),
                            'username': parser.get(section, 'username', fallback=None),
                            'password': parser.get(section, 'password', fallback=None),
                            'database': parser.get(section, 'database', fallback=None)
                        }
        self.variables = variables
        self.aliases = aliases
        return configs

    def connect(self, conn_name):
        cfg = self.configs[conn_name]
        try:
            if 'connection_string' in cfg and cfg['connection_string']:
                self.client = pymongo.MongoClient(cfg['connection_string'],datetime_conversion='DATETIME_AUTO', serverSelectionTimeoutMS=5000)
            else:
                uri = f"mongodb://{cfg['host']}:{cfg['port']}/"
                if cfg.get('username') and cfg.get('password'):
                    uri = f"mongodb://{cfg['username']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/"
                self.client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
            # Attempt to fetch server info to trigger connection
            self.client.server_info()
            self.db = self.client[cfg['database']]
            self.current_conn = conn_name
        except Exception as e:
            print(f"Connection to '{conn_name}' failed: {e}")
            self.client = None
            self.db = None
            self.current_conn = None

    def substitute_vars(self, text):
        for k, v in self.variables.items():
            text = text.replace(f"${k}", str(v))
        return text

    def run_session(self):
        print(f"Connected to: {self.current_conn}")
        session = PromptSession(history=FileHistory('mongo_cli_history.txt'))
        while True:
            try:
                line = session.prompt('mongo> ').strip()
                if not line:
                    continue
                # Multiple commands separated by ;
                commands = [cmd.strip() for cmd in line.split(';') if cmd.strip()]
                for cmd_line in commands:
                    # Alias expansion
                    for alias, cmd in self.aliases.items():
                        if cmd_line.lower().startswith(alias.lower()):
                            extra = cmd_line[len(alias):].strip()
                            cmd_line = f"{cmd} {extra}".strip()
                    # Variable substitution
                    cmd_line = self.substitute_vars(cmd_line)
                    # Handle exit
                    if cmd_line.lower() in ['exit', 'quit']:
                        print("Bye!")
                        return
                    # Handle connection switching
                    if cmd_line.startswith('use '):
                        conn = cmd_line.split(' ', 1)[1].strip()
                        if conn in self.configs:
                            self.connect(conn)
                            print(f"Switched to: {conn}")
                        else:
                            print(f"Connection '{conn}' not found.")
                        continue
                    # Show connections
                    if cmd_line == 'show connections':
                        print("Configured connections:")
                        for conn in self.configs:
                            print(f"  {conn}")
                        continue
                    # Show variables
                    if cmd_line == 'show vars':
                        print("Session variables:")
                        for k, v in self.variables.items():
                            print(f"  {k} = {v}")
                        continue
                    # Set variable
                    if cmd_line.startswith('set '):
                        parts = cmd_line[4:].split('=', 1)
                        if len(parts) == 2:
                            k, v = parts[0].strip(), parts[1].strip()
                            self.variables[k] = v
                            print(f"Set {k} = {v}")
                        continue
                    # Alias definition
                    if cmd_line.startswith('alias '):
                        parts = cmd_line[6:].split('=', 1)
                        if len(parts) == 2:
                            k, v = parts[0].strip(), parts[1].strip()
                            self.aliases[k] = v
                            print(f"Alias {k} = {v}")
                        continue
                    # Handle piping and redirection
                    if '|' in cmd_line or '>' in cmd_line:
                        self.handle_pipe_redirect(cmd_line)
                        continue
                    # Execute MongoDB command
                    self.execute_command(cmd_line)
            except KeyboardInterrupt:
                print("\nBye!")
                break
            except Exception as e:
                print(f"Error: {e}")

    def handle_pipe_redirect(self, line):
        if '>' in line:
            cmd, filename = line.split('>', 1)
            cmd = cmd.strip()
            filename = filename.strip()
            result = self.execute_command(cmd, return_result=True)
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(json_util.dumps(result, indent=2, ensure_ascii=False))
            print(f"Output written to {filename}")
        elif '|' in line:
            cmd, pipe_cmd = line.split('|', 1)
            cmd = cmd.strip()
            pipe_cmd = pipe_cmd.strip()
            result = self.execute_command(cmd, return_result=True)
            proc = subprocess.Popen(shlex.split(pipe_cmd), stdin=subprocess.PIPE)
            proc.communicate(input=json.dumps(result, indent=2, ensure_ascii=False).encode('utf-8'))

    def execute_command(self, command, return_result=False):
        # SQL translation
        if command.strip().upper().startswith("SELECT"):
            sql_result = sql_to_mongo(command)
            if sql_result:
                collection, method, args = sql_result
                coll = self.db[collection]
                result = None
                if method == 'find':
                    cursor = coll.find(*args)
                    result = list(cursor)
                    print(json_util.dumps(result, indent=2, ensure_ascii=False))
                    if return_result:
                        return result
                return
            else:
                return
        # Handle db command
        if command.strip() == 'db':
            print(self.db.name)
            if return_result:
                return self.db.name
            return
        # Handle find, insert, update, delete
        try:
            if command.startswith('db.'):
                # Parse collection and method
                rest = command[3:]
                if '.' not in rest:
                    print("Invalid command.")
                    return
                collection, rest = rest.split('.', 1)
                if '(' not in rest or not rest.endswith(')'):
                    print("Invalid command.")
                    return
                method, argstr = rest.split('(', 1)
                argstr = argstr[:-1]  # Remove trailing ')'
                args = []
                if argstr.strip():
                    # Try to parse as JSON or Python dict
                    try:
                        args = [json.loads(argstr)]
                    except Exception:
                        try:
                            args = [eval(argstr)]
                        except Exception:
                            print("Invalid argument format.")
                            return
                coll = self.db[collection]
                result = None
                if method == 'find':
                    cursor = coll.find(*args)
                    result = list(cursor)
                    print(json_util.dumps(result, indent=2, ensure_ascii=False))
                elif method == 'insert_one':
                    result = coll.insert_one(*args)
                    print(f"Inserted: {result.inserted_id}")
                    result = {"inserted_id": str(result.inserted_id)}
                elif method == 'delete_one':
                    result = coll.delete_one(*args)
                    print(f"Deleted: {result.deleted_count}")
                    result = {"deleted_count": result.deleted_count}
                elif method == 'update_one':
                    result = coll.update_one(*args)
                    print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")
                    result = {"matched_count": result.matched_count, "modified_count": result.modified_count}
                else:
                    print("Unsupported method.")
                    return
                if return_result:
                    return result
            else:
                print("Unknown command.")
        except Exception as e:
            print(f"MongoDB error: {e}")

def sql_to_mongo(sql):
    # Example: SELECT * FROM users WHERE age > 21 ORDER BY age DESC LIMIT 5
    # Regex to capture SELECT ... FROM ... [WHERE ...] [ORDER BY ...] [LIMIT ...]
    match = re.match(
        r"SELECT\s+(.+)\s+FROM\s+(\w+)"
        r"(?:\s+WHERE\s+(.+?))?"
        r"(?:\s+ORDER\s+BY\s+([\w\.]+)(?:\s+(ASC|DESC))?)?"
        r"(?:\s+LIMIT\s+(\d+))?$",
        sql, re.IGNORECASE
    )
    if not match:
        print("Unsupported SQL syntax.")
        return None

    fields = match.group(1).replace(' ', '').split(',')
    collection = match.group(2)
    where_clause = match.group(3)
    order_by_field = match.group(4)
    order_by_dir = match.group(5)
    limit_val = match.group(6)

    filter_doc = {}
    projection = None
    sort = None
    limit = None

    # Handle SELECT * (no projection)
    if len(fields) == 1 and fields[0] == '*':
        projection = None
    else:
        projection = {field: 1 for field in fields}

    if where_clause:
        # Supports AND, =, !=, >, <, >=, <=, and boolean values
        conditions = [c.strip() for c in re.split(r"\s+AND\s+", where_clause, flags=re.IGNORECASE)]
        for cond in conditions:
            eq_match = re.match(r"([\w\.]+)\s*=\s*'([^']*)'", cond)
            if eq_match:
                key = eq_match.group(1)
                value = eq_match.group(2)
                filter_doc[key] = value
                continue
            eq_match = re.match(r'([\w\.]+)\s*=\s*"([^"]*)"', cond)
            if eq_match:
                key = eq_match.group(1)
                value = eq_match.group(2)
                filter_doc[key] = value
                continue
            bool_match = re.match(r"([\w\.]+)\s*=\s*(true|false)", cond, re.IGNORECASE)
            if bool_match:
                key = bool_match.group(1)
                value = bool_match.group(2).lower() == "true"
                filter_doc[key] = value
                continue
            ne_match = re.match(r"([\w\.]+)\s*!=\s*'([^']*)'", cond)
            if ne_match:
                key = ne_match.group(1)
                value = ne_match.group(2)
                filter_doc[key] = {"$ne": value}
                continue
            ne_match = re.match(r'([\w\.]+)\s*!=\s*"([^"]*)"', cond)
            if ne_match:
                key = ne_match.group(1)
                value = ne_match.group(2)
                filter_doc[key] = {"$ne": value}
                continue
            ne_bool_match = re.match(r"([\w\.]+)\s*!=\s*(true|false)", cond, re.IGNORECASE)
            if ne_bool_match:
                key = ne_bool_match.group(1)
                value = ne_bool_match.group(2).lower() == "true"
                filter_doc[key] = {"$ne": value}
                continue
            gt_match = re.match(r"([\w\.]+)\s*>\s*([^\s]+)", cond)
            if gt_match:
                key = gt_match.group(1)
                value = gt_match.group(2)
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                filter_doc[key] = {"$gt": value}
                continue
            lt_match = re.match(r"([\w\.]+)\s*<\s*([^\s]+)", cond)
            if lt_match:
                key = lt_match.group(1)
                value = lt_match.group(2)
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                filter_doc[key] = {"$lt": value}
                continue
            gte_match = re.match(r"([\w\.]+)\s*>=\s*([^\s]+)", cond)
            if gte_match:
                key = gte_match.group(1)
                value = gte_match.group(2)
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                filter_doc[key] = {"$gte": value}
                continue
            lte_match = re.match(r"([\w\.]+)\s*<=\s*([^\s]+)", cond)
            if lte_match:
                key = lte_match.group(1)
                value = lte_match.group(2)
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                filter_doc[key] = {"$lte": value}
                continue
            print(f"Unsupported WHERE condition: {cond}")

    # ORDER BY
    if order_by_field:
        direction = -1 if order_by_dir and order_by_dir.upper() == "DESC" else 1
        sort = [(order_by_field, direction)]

    # LIMIT
    if limit_val:
        limit = int(limit_val)

    # Return all options for execution
    # [filter, projection, sort, limit]
    args = [filter_doc]
    if projection is not None:
        args.append(projection)
    return collection, 'find', args, sort, limit