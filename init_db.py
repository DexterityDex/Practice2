from config import app
from models import db, ma

# Инициализация приложения с базой данных и marshmallow
db.init_app(app)
ma.init_app(app)

# Создание таблиц базы данных
def init_database():
    with app.app_context():
        db.create_all()

if __name__ == "__main__":
    init_database()
    print("База данных инициализирована")