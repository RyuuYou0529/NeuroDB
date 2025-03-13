from neurodb import NeuroDB

def test():
    db_config = {
        'db_name': 'dendrites',
        'db_url': 'localhost:3306',
        'username': 'root',
        'password': 'neurodb'
    }
    neurodb = NeuroDB(db_name=db_config['db_name'], config=db_config)

if __name__ == '__main__':
    test()