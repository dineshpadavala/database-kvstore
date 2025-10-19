import sys
import os
import logging
from typing import Optional

# ─────────────────────────────────────────────────────────────
# Logging configuration
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────
DATA_FILE = 'data.db'   # Append-only persistence file
CMD_SET = 'SET'
CMD_GET = 'GET'
CMD_EXIT = 'EXIT'

# In-memory key-value store
store: dict[str, str] = {}


# ─────────────────────────────────────────────────────────────
# Persistence & Data Loading Functions
# ─────────────────────────────────────────────────────────────
def load_store() -> None:
    """
    Load the persistent log file into memory.
    Replays historical SET operations (last write wins).
    """
    if not os.path.exists(DATA_FILE):
        return

    try:
        with open(DATA_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith(f"{CMD_SET} "):
                    parts = line.split(None, 2)
                    if len(parts) == 3:
                        key, value = parts[1], parts[2]
                        store[key] = value
    except (IOError, OSError) as e:
        logging.error(f"Error loading data file: {e}")


def save_set(key: str, value: str) -> None:
    """
    Writes a SET command to disk (append-only with durability),
    then updates in-memory store.
    """
    try:
        with open(DATA_FILE, 'a') as f:
            f.write(f"{CMD_SET} {key} {value}\n")
            f.flush()
            os.fsync(f.fileno())
        store[key] = value
    except (IOError, OSError) as e:
        logging.error(f"Error writing to data file: {e}")


def get_value(key: str) -> Optional[str]:
    """
    Retrieves the value for a key.
    Returns:
        str if key exists, otherwise None.
    """
    return store.get(key)


# ─────────────────────────────────────────────────────────────
# Main CLI Loop
# ─────────────────────────────────────────────────────────────
def main() -> None:
    load_store()
    interactive = sys.stdin.isatty()

    if interactive:
        print("--- Simple Key-Value Store ---", file=sys.stderr)
        print("Commands: SET <key> <value>, GET <key>, EXIT", file=sys.stderr)

    while True:
        try:
            line = input("db> " if interactive else "")
        except (EOFError, KeyboardInterrupt):
            print("\nExiting...", file=sys.stderr)
            break

        if not line:
            continue

        parts = line.strip().split(None, 2)

        # GET <key>
        if len(parts) == 2 and parts[0] == CMD_GET:
            value = get_value(parts[1])
            print(value if value is not None else "")

        # SET <key> <value>
        elif len(parts) >= 3 and parts[0] == CMD_SET:
            key, value = parts[1], parts[2]
            save_set(key, value)
            if interactive:
                print("(OK)", file=sys.stderr)

        # EXIT
        elif parts[0] == CMD_EXIT:
            break

        else:
            if interactive:
                print("Invalid command. Use SET, GET, or EXIT.", file=sys.stderr)


if __name__ == '__main__':
    main()
