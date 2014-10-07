import os

ENVIRONMENT_CMD_KEYS = {
    "0.20.2" : {"port": "hadoop.pipes.command.port", "file": "hadoop.pipes.command.port", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "0.20.2-cdh3u4" : {"port": "hadoop.pipes.command.port", "file": "hadoop.pipes.command.port", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "0.20.2-cdh3u5" : {"port": "hadoop.pipes.command.port", "file": "hadoop.pipes.command.port", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "1.0.4" : {"port": "hadoop.pipes.command.port", "file": "hadoop.pipes.command.port", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "1.0.4.patched" : {"port": "hadoop.pipes.command.port", "file": "hadoop.pipes.command.port", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "1.1.2" : {"port": "hadoop.pipes.command.port", "file": "hadoop.pipes.command.port", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "1.2.1" : {"port": "hadoop.pipes.command.port", "file": "hadoop.pipes.command.port", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "2.0.0-cdh4.2.0" : {"port": "mapreduce.pipes.command.port", "file": "hadoop.pipes.commandport", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "2.0.0-cdh4.3.0" : {"port": "mapreduce.pipes.command.port", "file": "hadoop.pipes.commandport", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "2.0.0-mr1-cdh4.2.0" : {"port": "mapreduce.pipes.command.port", "file": "hadoop.pipes.commandport", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "2.0.0-mr1-cdh4.3.0" : {"port": "mapreduce.pipes.command.port", "file": "hadoop.pipes.commandport", "secret_location":  "hadoop.pipes.shared.secret.location"},
    "2.2.0" : {"port": "mapreduce.pipes.command.port", "file": "hadoop.pipes.commandport", "secret_location":  "hadoop.pipes.shared.secret.location"}
}



MAPREDUCE_TASK_IO_SORT_MB_KEY = "mapreduce.task.io.sort.mb"
MAPREDUCE_TASK_IO_SORT_MB = 100


def resolve_environment_port_key():
    key = set(os.environ.keys()).intersection(set([v["port"] for v in ENVIRONMENT_CMD_KEYS.itervalues()]))
    return None if len(key) is 0 else key.pop()


def resolve_environment_port(key=None):
    if key is None:
        key = resolve_environment_port_key()
    return None if key is None else os.getenv(key)


def resolve_environment_file_key():
    key = set(os.environ.keys()).intersection(set([v["file"] for v in ENVIRONMENT_CMD_KEYS.itervalues()]))
    return None if len(key) is 0 else key.pop()


def resolve_environment_file(key=None):
    if key is None:
        key = resolve_environment_file_key()
    return None if key is None else os.getenv(key)


def resolve_environment_secret_location_key():
    key = set(os.environ.keys()).intersection(set([v["secret_location"] for v in ENVIRONMENT_CMD_KEYS.itervalues()]))
    return "hadoop.pipes.shared.secret.location" if len(key) is 0 else key.pop()


def resolve_environment_secret_location(key=None):
    if key is None:
        key = resolve_environment_secret_location_key()
    return None if key is None else os.getenv(key)
