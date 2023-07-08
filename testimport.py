if __name__ == '__main__':
    try:
        from datetime import datetime

        print(datetime.strptime("2017-10-01T09:16:46", '%Y-%m-%dT%H:%M:%S').month)
    except ImportError:
        print("Import failed")
