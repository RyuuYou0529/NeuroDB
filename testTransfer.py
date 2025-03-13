from neurodb import sqlite2dj

def test_sqlite2dj():
    db_path_sqlite = 'test/test_data/dendrites.db'
    db_config_dj = {
        'db_name': 'dendrites',
        'db_url': 'localhost:3306',
        'username': 'root',
        'password': 'neurodb'
    }
    result = sqlite2dj(db_path_sqlite, db_config_dj)
    print(result)

if __name__ == "__main__":
    test_sqlite2dj()