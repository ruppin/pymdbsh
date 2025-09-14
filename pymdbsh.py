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
        session = PromptSession(history=FileHistory('mongo_cli_history.txt'))
        while True:
            try:
                # Show connection and DB in prompt
                prompt_str = f"[{self.current_conn}/{self.db.name if self.db is not None else '?'}] mongo> "
                line = session.prompt(prompt_str).strip()
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
                    # Command substitution
                    cmd_line = self.substitute_commands(cmd_line)
                    #clear screen
                    if cmd_line.lower() == 'clear':
                        os.system('cls' if os.name == 'nt' else 'clear')
                        continue
                    # Handle exit

                    if cmd_line.lower() in ['exit', 'quit']:
                        print("Bye!")
                        return
                    # Handle connection switching with 'switch'
                    if cmd_line.startswith('switch '):
                        conn = cmd_line.split(' ', 1)[1].strip()
                        if conn in self.configs:
                            self.connect(conn)
                            print(f"Switched to: {conn}")
                        else:
                            print(f"Connection '{conn}' not found.")
                        continue
                    # Handle 'use' for connection or database
                    if cmd_line.startswith('use '):
                        name = cmd_line.split(' ', 1)[1].strip()
                        # First, check if it's a connection name
                        if name in self.configs:
                            self.connect(name)
                            print(f"Switched to connection: {name}")
                        else:
                            # Try to switch database within the current connection
                            if self.client:
                                try:
                                    self.db = self.client[name]
                                    print(f"Switched to database: {name}")
                                except Exception:
                                    print(f"No database by the name '{name}' in the current connection.")
                            else:
                                print(f"No connection or database by the name '{name}'.")
                        continue
                    # Show connections
                    if cmd_line == 'show connections':
                        print("Configured connections:")
                        for conn in self.configs:
                            marker = " (current)" if conn == self.current_conn else ""
                            print(f"  {conn}{marker}")
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
        import re

        def is_outside_quotes_and_parens(s, idx):
            in_single = in_double = False
            paren_level = 0
            for i, c in enumerate(s):
                if c == "'" and not in_double:
                    in_single = not in_single
                elif c == '"' and not in_single:
                    in_double = not in_double
                elif c == '(' and not in_single and not in_double:
                    paren_level += 1
                elif c == ')' and not in_single and not in_double:
                    paren_level = max(paren_level - 1, 0)
                if i == idx:
                    return not in_single and not in_double and paren_level == 0
            return True

        # Find the last > that is outside quotes and parentheses
        gt_indices = [m.start() for m in re.finditer('>', line)]
        split_idx = None
        for idx in reversed(gt_indices):
            if is_outside_quotes_and_parens(line, idx):
                split_idx = idx
                break

        if split_idx is not None:
            cmd = line[:split_idx].strip()
            filename = line[split_idx+1:].strip()
            result = self.execute_command(cmd, return_result=True, suppress_output=True)
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(json_util.dumps(result, indent=2, ensure_ascii=False))
            print(f"Output written to {filename}")
            return

        # Fallback to pipe handling
        if '|' in line:
            cmd, pipe_cmd = line.split('|', 1)
            cmd = cmd.strip()
            pipe_cmd = pipe_cmd.strip()
            result = self.execute_command(cmd, return_result=True, suppress_output=True)
            proc = subprocess.Popen(shlex.split(pipe_cmd), stdin=subprocess.PIPE)
            proc.communicate(input=json_util.dumps(result, indent=2, ensure_ascii=False).encode('utf-8'))

    def execute_command(self, command, return_result=False, suppress_output=False):
        # SQL translation
        if command.strip().upper().startswith("SELECT"):
            sql_result = sql_to_mongo(command)
            if sql_result:
                collection, method, args, sort, limit = sql_result
                coll = self.db[collection]
                result = None
                if method == 'find':
                    cursor = coll.find(*args)
                    if sort:
                        cursor = cursor.sort(sort)
                    if limit:
                        cursor = cursor.limit(limit)
                    result = list(cursor)
                    if not suppress_output:
                        print(json_util.dumps(result, indent=2, ensure_ascii=False))
                    if return_result:
                        return result
                if method == 'aggregate':
                    cursor = coll.aggregate(*args)
                    result = list(cursor)
                    if not suppress_output:
                        print(json_util.dumps(result, indent=2, ensure_ascii=False))
                    if return_result:
                        return result
                return
        # Handle db command
        if command.strip() == 'db':
            if not suppress_output:
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
                    if not suppress_output:
                        print("Invalid command.")
                    return
                collection, rest = rest.split('.', 1)
                if '(' not in rest or not rest.endswith(')'):
                    if not suppress_output:
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
                            if not suppress_output:
                                print("Invalid argument format.")
                            return
                coll = self.db[collection]
                result = None
                if method == 'find':
                    cursor = coll.find(*args)
                    result = list(cursor)
                    if not suppress_output:
                        print(json_util.dumps(result, indent=2, ensure_ascii=False))
                elif method == 'insert_one':
                    result = coll.insert_one(*args)
                    if not suppress_output:
                        print(f"Inserted: {result.inserted_id}")
                    result = {"inserted_id": str(result.inserted_id)}
                elif method == 'delete_one':
                    result = coll.delete_one(*args)
                    if not suppress_output:
                        print(f"Deleted: {result.deleted_count}")
                    result = {"deleted_count": result.deleted_count}
                elif method == 'update_one':
                    result = coll.update_one(*args)
                    if not suppress_output:
                        print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")
                    result = {"matched_count": result.matched_count, "modified_count": result.modified_count}
                else:
                    if not suppress_output:
                        print("Unsupported method.")
                if return_result:
                    return result
            else:
                if not suppress_output:
                    print("Unknown command.")
        except Exception as e:
            if not suppress_output:
                print(f"MongoDB error: {e}")

    def substitute_commands(self, text):
        # Find all backtick-enclosed commands and replace with their output
        def repl(match):
            cmd = match.group(1)
            try:
                output = subprocess.check_output(cmd, shell=True, text=True)
                return output.strip()
            except Exception as e:
                return f"<error:{e}>"
        # Replace all `...` with output
        return re.sub(r'`([^`]+)`', repl, text)

def sql_to_mongo(sql):
    # Extended regex to capture JOIN
    join_match = re.match(
        r"SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+(\w+))?\s+JOIN\s+(\w+)(?:\s+(\w+))?\s+ON\s+([\w\.]+)\s*=\s*([\w\.]+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+([\w\.]+)(?:\s+(ASC|DESC))?)?(?:\s+LIMIT\s+(\d+))?$",
        sql, re.IGNORECASE
    )
    if join_match:
        fields = join_match.group(1).replace(' ', '').split(',')
        left_coll = join_match.group(2)
        left_alias = join_match.group(3) or left_coll
        right_coll = join_match.group(4)
        right_alias = join_match.group(5) or right_coll
        left_key = join_match.group(6)
        right_key = join_match.group(7)
        where_clause = join_match.group(8)
        order_by_field = join_match.group(9)
        order_by_dir = join_match.group(10)
        limit_val = join_match.group(11)
        print("join detected")
        # Build $lookup
        lookup_stage = {
            "$lookup": {
                "from": right_coll,
                "localField": left_key.split('.')[-1],
                "foreignField": right_key.split('.')[-1],
                "as": right_alias
            }
        }
        # Optionally, $unwind if you expect one-to-one
        unwind_stage = {"$unwind": f"${right_alias}"}

        # Build $project
        project = {}

        # Dynamically expand a.* and b.* using find_one()
        left_fields = []
        right_fields = []
        if len(fields) == 1 and fields[0] == '*':
            project = None
        else:
            from pymongo import MongoClient
            left_doc = None
            right_doc = None
            try:
                left_doc = cli.db[left_coll].find_one() if hasattr(cli, 'db') else None
            except Exception:
                pass
            try:
                right_doc = cli.db[right_coll].find_one() if hasattr(cli, 'db') else None
            except Exception:
                pass
            if left_doc:
                left_fields = [k for k in left_doc.keys() if k != '_id']
            if right_doc:
                right_fields = [k for k in right_doc.keys() if k != '_id']

            for field in fields:
                if field == f"{left_alias}.*":
                    for lf in left_fields:
                        project[lf] = 1
                elif field == f"{right_alias}.*":
                    for rf in right_fields:
                        project[f"{right_alias}.{rf}"] = 1
                elif '.' in field:
                    alias, fname = field.split('.', 1)
                    if alias == left_alias:
                        project[fname] = 1
                    elif alias == right_alias:
                        project[f"{right_alias}.{fname}"] = 1
                else:
                    project[field] = 1

        # Initialize pipeline before appending to it!
        pipeline = [lookup_stage, unwind_stage]

        # WHERE clause (only simple ANDs supported)
        if where_clause:
            filter_doc = {}
            conditions = [c.strip() for c in re.split(r"\s+AND\s+", where_clause, flags=re.IGNORECASE)]
            for cond in conditions:
                m = re.match(r"(\w+)\.(\w+)\s*=\s*'([^']*)'", cond)
                if m:
                    alias, key, value = m.groups()
                    filter_doc[f"{alias}.{key}"] = value
                    continue
                m = re.match(r"(\w+)\.(\w+)\s*=\s*(\d+)", cond)
                if m:
                    alias, key, value = m.groups()
                    filter_doc[f"{alias}.{key}"] = int(value)
                    continue
                # Add more condition parsing as needed
            if filter_doc:
                pipeline.append({"$match": filter_doc})

        if project:
            pipeline.append({"$project": project})

        # ORDER BY
        if order_by_field:
            direction = -1 if order_by_dir and order_by_dir.upper() == "DESC" else 1
            pipeline.append({"$sort": {order_by_field: direction}})
        # LIMIT
        if limit_val:
            pipeline.append({"$limit": int(limit_val)})

        return left_coll, 'aggregate', [pipeline], None, None

    # Fallback to original SELECT (no join)
    # Regex to capture SELECT ... FROM ... [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT ...]
    match = re.match(
        r"SELECT\s+(.+)\s+FROM\s+(\w+)"
        r"(?:\s+WHERE\s+(.+?))?"
        r"(?:\s+GROUP\s+BY\s+([\w\.]+))?"
        r"(?:\s+HAVING\s+(.+?))?"
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
    group_by_field = match.group(4)
    having_clause = match.group(5)
    order_by_field = match.group(6)
    order_by_dir = match.group(7)
    limit_val = match.group(8)

    filter_doc = {}
    projection = None
    sort = None
    limit = None

    # Handle SELECT * (no projection)
    if len(fields) == 1 and fields[0] == '*':
        projection = None
    else:
        projection = {field: 1 for field in fields if field != 'COUNT(*)'}

    # WHERE clause (add LIKE support)
    if where_clause:
        conditions = [c.strip() for c in re.split(r"\s+AND\s+", where_clause, flags=re.IGNORECASE)]
        for cond in conditions:
            # LIKE
            like_match = re.match(r"([\w\.]+)\s+LIKE\s+'([^']+)'", cond, re.IGNORECASE)
            if like_match:
                key = like_match.group(1)
                pattern = like_match.group(2)
                # Convert SQL LIKE pattern to regex
                regex = '^' + pattern.replace('%', '.*').replace('_', '.') + '$'
                filter_doc[key] = {"$regex": regex}
                continue
            # IS NULL
            is_null_match = re.match(r"([\w\.]+)\s+IS\s+NULL", cond, re.IGNORECASE)
            if is_null_match:
                key = is_null_match.group(1)
                filter_doc[key] = None
                continue
            # IS NOT NULL
            is_not_null_match = re.match(r"([\w\.]+)\s+IS\s+NOT\s+NULL", cond, re.IGNORECASE)
            if is_not_null_match:
                key = is_not_null_match.group(1)
                filter_doc[key] = {"$ne": None}
                continue
            # ...existing matches below...
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

    # GROUP BY support with HAVING
    if group_by_field:
        pipeline = []
        if where_clause and filter_doc:
            pipeline.append({"$match": filter_doc})
        group_stage = {
            "$group": {
                "_id": f"${group_by_field}",
                "count": {"$sum": 1}
            }
        }
        for field in fields:
            if field not in ('COUNT(*)', group_by_field):
                group_stage["$group"][field] = {"$first": f"${field}"}
        pipeline.append(group_stage)
        # HAVING support (only for COUNT(*) for now)
        if having_clause:
            having_match = re.match(r"COUNT\(\*\)\s*([<>=!]+)\s*(\d+)", having_clause.strip(), re.IGNORECASE)
            if having_match:
                op, value = having_match.group(1), int(having_match.group(2))
                mongo_op = {
                    '=': '$eq',
                    '==': '$eq',
                    '!=': '$ne',
                    '<>': '$ne',
                    '>': '$gt',
                    '>=': '$gte',
                    '<': '$lt',
                    '<=': '$lte'
                }.get(op)
                if mongo_op:
                    pipeline.append({"$match": {"count": {f"${mongo_op}": value}}})
        # Project output fields
        project_stage = {"$project": {group_by_field: "$_id", "count": 1}}
        for field in fields:
            if field not in ('COUNT(*)', group_by_field):
                project_stage["$project"][field] = 1
        pipeline.append(project_stage)
        # ORDER BY
        if order_by_field:
            direction = -1 if order_by_dir and order_by_dir.upper() == "DESC" else 1
            pipeline.append({"$sort": {order_by_field: direction}})
        # LIMIT
        if limit_val:
            pipeline.append({"$limit": int(limit_val)})
        return collection, 'aggregate', [pipeline], None, None

    # ORDER BY (for non-grouped queries)
    if order_by_field:
        direction = -1 if order_by_dir and order_by_dir.upper() == "DESC" else 1
        sort = [(order_by_field, direction)]

    # LIMIT (for non-grouped queries)
    if limit_val:
        limit = int(limit_val)

    # Return all options for execution
    args = [filter_doc]
    if projection is not None:
        args.append(projection)
    return collection, 'find', args, sort, limit
if __name__ == '__main__':
    cli = MongoCLI('~/.pymdbsh.conf')
    cli.run_session()

    #Join  syntax
    #SELECT a.*, b.name FROM users a JOIN orders b ON a.user_id = b.user_id WHERE a.status = 'active'