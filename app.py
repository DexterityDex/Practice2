from config import app
from init_db import init_database
from upload_db import init_data
from structures.views import *

if __name__ == '__main__':
    init_database()
    init_data()
    app.run(debug=True)