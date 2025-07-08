import logging
from ASL import ASL_Logging
from bbg_database import BloombergDatabase


def main():
    db_server: str = "asldb03"
    db_port : str = "1433"
    _database : str = "playdb"

    db_connection = BloombergDatabase(
            server=db_server, port=db_port, database=_database
        )

if __name__ == "__main__":
    main()