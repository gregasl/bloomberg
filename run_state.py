from enum import Enum, unique

@unique
class RunState(Enum):
    INITIALIZING = 0,
    RUNNING = 1,
    PAUSED = 2,
    RESUMING = 3,
    CMD_DIE = 4,
    ERROR_DIE = 5