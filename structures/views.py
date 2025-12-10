from flask import render_template, request, jsonify, make_response
from config import app
from models import db, ContentType, Country, Rating, NetflixContent
from sqlalchemy import func, desc, Integer
from structures.serializers import (
    content_type_schema, content_types_schema,
    country_schema, countries_schema,
    rating_schema, ratings_schema,
    content_schema, contents_schema
)
from werkzeug.exceptions import NotFound, BadRequest, InternalServerError


@app.template_filter('format_seasons')
def format_seasons_filter(number):
    return format_seasons(number)

def format_seasons(number):
    if number is None:
        return "неизвестно"

    number = int(number)

    last_digit = number % 10
    last_two_digits = number % 100

    if last_two_digits in range(11, 15):
        return f"{number} сезонов"
    elif last_digit == 1:
        return f"{number} сезон"
    elif last_digit in [2, 3, 4]:
        return f"{number} сезона"
    else:
        return f"{number} сезонов"

@app.route('/')
def index():
    # Получаем список всех типов контента, стран и рейтингов
    content_types = ContentType.query.all()
    countries = Country.query.all()
    ratings = Rating.query.all()

    # Запрос 1: Сериалы за последний год
    max_year = db.session.query(
        func.max(NetflixContent.release_year)
    ).filter(
        NetflixContent.release_year.isnot(None)
    ).scalar()

    query1_headers = ["Название", "Год выпуска", "Рейтинг", "Длительность"]
    query1_data = db.session.query(
        NetflixContent.title,
        NetflixContent.release_year,
        Rating.name,
        db.case(
            (ContentType.name == 'TV Show', db.cast(NetflixContent.duration_seasons, db.String)),
            else_='Неизвестно'
        ).label('duration'),
        ContentType.name.label('content_type')
    ).join(
        Rating, NetflixContent.rating_id == Rating.identifier
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        NetflixContent.release_year == max_year,
        db.or_(
            db.and_(ContentType.name == 'TV Show', NetflixContent.duration_seasons.isnot(None))
        )
    ).order_by(
        NetflixContent.title.asc()
    ).all()

    formatted_query1_data = []
    for title, year, rating, duration, content_type in query1_data:
        if content_type == 'TV Show' and duration != 'Неизвестно':
            try:
                formatted_duration = format_seasons(int(duration))
            except (ValueError, TypeError):
                formatted_duration = duration
        else:
            formatted_duration = duration

        formatted_query1_data.append((title, year, rating, formatted_duration))

    # Запрос 2: Страны-лидеры по количеству контента
    query2_headers = ["Страна", "Количество контента"]
    query2_data = db.session.query(
        Country.name,
        func.count(NetflixContent.show_id)
    ).join(
        NetflixContent, NetflixContent.country_id == Country.identifier
    ).group_by(
        Country.name
    ).having(
        func.count(NetflixContent.show_id) > 100
    ).order_by(
        func.count(NetflixContent.show_id).desc()
    ).all()

    # Запрос 3: Топ 1% фильмов с самой большой продолжительностью
    query3_headers = ["Название", "Год", "Длительность (мин)"]

    query3_data = db.session.query(
        NetflixContent.title,
        NetflixContent.release_year,
        NetflixContent.duration_minutes
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None)
    ).order_by(
        NetflixContent.duration_minutes.desc()
    ).limit(
        db.session.query(
            func.cast(func.count(NetflixContent.show_id) * 0.01, Integer)
        ).join(
            ContentType, NetflixContent.type_id == ContentType.identifier
        ).filter(
            ContentType.name == 'Movie',
            NetflixContent.duration_minutes.isnot(None)
        ).scalar_subquery()
    ).all()

    # Запрос 4: Количество добавлений по годам
    query4_headers = ["Год добавления", "Фильмов", "Сериалов", "Всего"]

    year_extract = func.strftime('%Y', NetflixContent.date_added).label('year_added')

    movies_subquery = db.session.query(
        year_extract.label('year'),
        func.count(NetflixContent.show_id).label('movie_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).subquery()

    series_subquery = db.session.query(
        year_extract.label('year'),
        func.count(NetflixContent.show_id).label('series_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'TV Show',
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).subquery()

    total_subquery = db.session.query(
        year_extract.label('year'),
        func.count(NetflixContent.show_id).label('total_count')
    ).filter(
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).subquery()

    years_list = db.session.query(
        year_extract.label('year')
    ).filter(
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).order_by(
        year_extract.desc()
    ).subquery()

    query4_data = db.session.query(
        years_list.c.year,
        func.coalesce(movies_subquery.c.movie_count, 0).label('movie_count'),
        func.coalesce(series_subquery.c.series_count, 0).label('series_count'),
        func.coalesce(total_subquery.c.total_count, 0).label('total_count')
    ).outerjoin(
        movies_subquery, years_list.c.year == movies_subquery.c.year
    ).outerjoin(
        series_subquery, years_list.c.year == series_subquery.c.year
    ).outerjoin(
        total_subquery, years_list.c.year == total_subquery.c.year
    ).order_by(
        years_list.c.year.desc()
    ).all()

    # Запрос 5: Средняя длительность фильмов по годам выпуска
    query5_headers = ["Год выпуска", "Средняя длительность (мин)", "Количество фильмов"]
    query5_data = db.session.query(
        NetflixContent.release_year,
        func.avg(NetflixContent.duration_minutes).label('avg_duration'),
        func.count(NetflixContent.show_id).label('movie_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None),
        NetflixContent.release_year.isnot(None)
    ).group_by(
        NetflixContent.release_year
    ).order_by(
        NetflixContent.release_year.desc()
    )

    return render_template('index.html',
                           content_types=content_types,
                           countries=countries,
                           ratings=ratings,
                           query1=[query1_headers, formatted_query1_data],
                           query2=[query2_headers, query2_data],
                           query3=[query3_headers, query3_data],
                           query4=[query4_headers, query4_data],
                           query5=[query5_headers, query5_data])


# ContentType API Endpoints
# curl -i http://127.0.0.1:5000/api/content-types
@app.route('/api/content-types', methods=['GET'])
def get_content_types():
    all_content_types = ContentType.query.all()
    result = content_types_schema.dump(all_content_types)
    return jsonify(result)

# curl -i http://127.0.0.1:5000/api/content-types/<id>
@app.route('/api/content-types/<int:id>', methods=['GET'])
def get_content_type(id):
    content_type = ContentType.query.get_or_404(id)
    return content_type_schema.jsonify(content_type)

# curl -i -H "Content-Type: application/json" -X POST http://127.0.0.1:5000/api/content-types -d "{\"name\": \"Documentary\"}"
@app.route('/api/content-types', methods=['POST'])
def add_content_type():
    name = request.json['name']
    new_content_type = ContentType(name)
    db.session.add(new_content_type)
    db.session.commit()
    return content_type_schema.jsonify(new_content_type)

# curl -i -H "Content-Type: application/json" -X PUT http://127.0.0.1:5000/api/content-types/<id> -d "{\"name\": \"Updated Content Type\"}"
@app.route('/api/content-types/<int:id>', methods=['PUT'])
def update_content_type(id):
    content_type = ContentType.query.get_or_404(id)
    content_type.name = request.json['name']
    db.session.commit()
    return content_type_schema.jsonify(content_type)

# curl -i -X DELETE http://127.0.0.1:5000/api/content-types/<id>
@app.route('/api/content-types/<int:id>', methods=['DELETE'])
def delete_content_type(id):
    content_type = ContentType.query.get_or_404(id)
    db.session.delete(content_type)
    db.session.commit()
    return jsonify({'message': 'Content type deleted'})


# Country API Endpoints
# curl -i http://127.0.0.1:5000/api/countries
@app.route('/api/countries', methods=['GET'])
def get_countries():
    all_countries = Country.query.all()
    result = countries_schema.dump(all_countries)
    return jsonify(result)

# curl -i http://127.0.0.1:5000/api/countries/<id>
@app.route('/api/countries/<int:id>', methods=['GET'])
def get_country(id):
    country = Country.query.get_or_404(id)
    return country_schema.jsonify(country)

# curl -i -H "Content-Type: application/json" -X POST http://127.0.0.1:5000/api/countries -d "{\"name\": \"New Country\"}"
@app.route('/api/countries', methods=['POST'])
def add_country():
    name = request.json['name']
    new_country = Country(name)
    db.session.add(new_country)
    db.session.commit()
    return country_schema.jsonify(new_country)

# curl -i -H "Content-Type: application/json" -X PUT http://127.0.0.1:5000/api/countries/<id> -d "{\"name\": \"Updated Country\"}"
@app.route('/api/countries/<int:id>', methods=['PUT'])
def update_country(id):
    country = Country.query.get_or_404(id)
    country.name = request.json['name']
    db.session.commit()
    return country_schema.jsonify(country)

# curl -i -X DELETE http://127.0.0.1:5000/api/countries/<id>
@app.route('/api/countries/<int:id>', methods=['DELETE'])
def delete_country(id):
    country = Country.query.get_or_404(id)
    db.session.delete(country)
    db.session.commit()
    return jsonify({'message': 'Country deleted'})


# Rating API Endpoints
# curl -i http://127.0.0.1:5000/api/ratings
@app.route('/api/ratings', methods=['GET'])
def get_ratings():
    all_ratings = Rating.query.all()
    result = ratings_schema.dump(all_ratings)
    return jsonify(result)

# curl -i http://127.0.0.1:5000/api/ratings/<id>
@app.route('/api/ratings/<int:id>', methods=['GET'])
def get_rating(id):
    rating = Rating.query.get_or_404(id)
    return rating_schema.jsonify(rating)

# curl -i -H "Content-Type: application/json" -X POST http://127.0.0.1:5000/api/ratings -d "{\"name\": \"New Rating\"}"
@app.route('/api/ratings', methods=['POST'])
def add_rating():
    name = request.json['name']
    new_rating = Rating(name)
    db.session.add(new_rating)
    db.session.commit()
    return rating_schema.jsonify(new_rating)

# curl -i -H "Content-Type: application/json" -X PUT http://127.0.0.1:5000/api/ratings/<id> -d "{\"name\": \"Updated Rating\"}"
@app.route('/api/ratings/<int:id>', methods=['PUT'])
def update_rating(id):
    rating = Rating.query.get_or_404(id)
    rating.name = request.json['name']
    db.session.commit()
    return rating_schema.jsonify(rating)

# curl -i -X DELETE http://127.0.0.1:5000/api/ratings/<id>
@app.route('/api/ratings/<int:id>', methods=['DELETE'])
def delete_rating(id):
    rating = Rating.query.get_or_404(id)
    db.session.delete(rating)
    db.session.commit()
    return jsonify({'message': 'Rating deleted'})


# Netflix Content API Endpoints
# curl -i http://127.0.0.1:5000/api/content
@app.route('/api/content', methods=['GET'])
def get_all_content():
    all_content = NetflixContent.query.all()
    result = contents_schema.dump(all_content)
    return jsonify(result)

# curl -i http://127.0.0.1:5000/api/content/<show_id>
@app.route('/api/content/<string:show_id>', methods=['GET'])
def get_content(show_id):
    content = NetflixContent.query.get_or_404(show_id)
    return content_schema.jsonify(content)

# curl -i -H "Content-Type: application/json" -X POST http://127.0.0.1:5000/api/content -d "{\"show_id\": \"1\", \"title\": \"New Show\", \"type_id\": 1, \"director\": \"Director Name\", \"cast\": \"Cast Name\", \"country_id\": 1, \"date_added\": \"2023-01-01\", \"release_year\": 2023, \"rating_id\": 1, \"duration_minutes\": 120, \"duration_seasons\": 3}"
@app.route('/api/content', methods=['POST'])
def add_content():
    data = request.json
    new_content = NetflixContent(
        show_id=data['show_id'],
        title=data['title'],
        type_id=data['type_id'],
        director=data.get('director'),
        cast=data.get('cast'),
        country_id=data.get('country_id'),
        date_added=data.get('date_added'),
        release_year=data.get('release_year'),
        rating_id=data.get('rating_id'),
        duration_minutes=data.get('duration_minutes'),
        duration_seasons=data.get('duration_seasons')
    )
    db.session.add(new_content)
    db.session.commit()
    return content_schema.jsonify(new_content)

# curl -i -H "Content-Type: application/json" -X PUT http://127.0.0.1:5000/api/content/<show_id> -d "{\"title\": \"Updated Show\", \"director\": \"New Director\"}"
@app.route('/api/content/<string:show_id>', methods=['PUT'])
def update_content(show_id):
    content = NetflixContent.query.get_or_404(show_id)
    data = request.json

    content.title = data.get('title', content.title)
    content.type_id = data.get('type_id', content.type_id)
    content.director = data.get('director', content.director)
    content.cast = data.get('cast', content.cast)
    content.country_id = data.get('country_id', content.country_id)
    content.date_added = data.get('date_added', content.date_added)
    content.release_year = data.get('release_year', content.release_year)
    content.rating_id = data.get('rating_id', content.rating_id)
    content.duration_minutes = data.get('duration_minutes', content.duration_minutes)
    content.duration_seasons = data.get('duration_seasons', content.duration_seasons)

    db.session.commit()
    return content_schema.jsonify(content)

# curl -i -X DELETE http://127.0.0.1:5000/api/content/<show_id>
@app.route('/api/content/<string:show_id>', methods=['DELETE'])
def delete_content(show_id):
    content = NetflixContent.query.get_or_404(show_id)
    db.session.delete(content)
    db.session.commit()
    return jsonify({'message': 'Content deleted'})

# curl -i http://127.0.0.1:5000/api/stats/content-by-country
@app.route('/api/stats/content-by-country', methods=['GET'])
def content_by_country():
    stats = db.session.query(
        Country.name,
        func.count(NetflixContent.show_id).label('content_count')
    ).join(
        NetflixContent, NetflixContent.country_id == Country.identifier
    ).group_by(
        Country.name
    ).order_by(
        func.count(NetflixContent.show_id).desc()
    ).all()

    return jsonify([{'country': name, 'content_count': count} for name, count in stats])


# curl -i http://127.0.0.1:5000/api/stats/min-max-avg-duration
@app.route('/api/stats/min-max-avg-duration', methods=['GET'])
def min_max_avg_duration():
    stats = db.session.query(
        NetflixContent.release_year,
        func.min(NetflixContent.duration_minutes).label('min_duration'),
        func.max(NetflixContent.duration_minutes).label('max_duration'),
        func.avg(NetflixContent.duration_minutes).label('avg_duration'),
        func.count(NetflixContent.show_id).label('movie_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None),
        NetflixContent.release_year.isnot(None)
    ).group_by(
        NetflixContent.release_year
    ).order_by(
        NetflixContent.release_year.desc()
    ).all()

    return jsonify([{
        'release_year': year,
        'min_duration': min_dur,
        'max_duration': max_dur,
        'avg_duration': float(avg_dur),
        'movie_count': count
    } for year, min_dur, max_dur, avg_dur, count in stats])


# curl -i http://127.0.0.1:5000/api/stats/content-by-type-and-rating
@app.route('/api/stats/content-by-type-and-rating', methods=['GET'])
def content_by_type_and_rating():
    stats = db.session.query(
        ContentType.name.label('type_name'),
        Rating.name.label('rating_name'),
        func.count(NetflixContent.show_id).label('content_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).join(
        Rating, NetflixContent.rating_id == Rating.identifier
    ).group_by(
        ContentType.name,
        Rating.name
    ).order_by(
        ContentType.name,
        func.count(NetflixContent.show_id).desc()
    ).all()

    return jsonify([{
        'type': type_name,
        'rating': rating_name,
        'content_count': count
    } for type_name, rating_name, count in stats])


# Дополнительные запросы для агрегирования данных

# curl -i http://127.0.0.1:5000/api/stats/avg-duration
@app.route('/api/stats/avg-duration', methods=['GET'])
def avg_duration():
    avg_duration = db.session.query(
        func.avg(NetflixContent.duration_minutes).label('avg_duration')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None)
    ).scalar()

    return jsonify({'avg_duration': avg_duration})

# curl -i http://127.0.0.1:5000/api/stats/min-duration
@app.route('/api/stats/min-duration', methods=['GET'])
def min_duration():
    min_duration = db.session.query(
        func.min(NetflixContent.duration_minutes).label('min_duration')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None)
    ).scalar()

    return jsonify({'min_duration': min_duration})

# curl -i http://127.0.0.1:5000/api/stats/max-duration
@app.route('/api/stats/max-duration', methods=['GET'])
def max_duration():
    max_duration = db.session.query(
        func.max(NetflixContent.duration_minutes).label('max_duration')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None)
    ).scalar()

    return jsonify({'max_duration': max_duration})


# curl -i http://127.0.0.1:5000/api/stats/rating-content-stats
@app.route('/api/stats/rating-content-stats', methods=['GET'])
def rating_content_stats():

    stats = db.session.query(
        Rating.name.label('rating_name'),
        func.count(NetflixContent.show_id).label('content_count')
    ).join(
        NetflixContent, NetflixContent.rating_id == Rating.identifier
    ).group_by(
        Rating.name
    ).all()

    if not stats:
        return jsonify({
            'min_content_count': 0,
            'max_content_count': 0,
            'avg_content_count': 0,
            'total_ratings': 0
        })

    content_counts = [count for _, count in stats]
    
    return jsonify({
        'min_content_count': min(content_counts),
        'max_content_count': max(content_counts),
        'avg_content_count': float(sum(content_counts)) / len(content_counts),
        'total_ratings': len(stats),
        'details': [{'rating': rating_name, 'content_count': count} for rating_name, count in stats]
    })


# curl -i http://127.0.0.1:5000/api/stats/country-content-stats
@app.route('/api/stats/country-content-stats', methods=['GET'])
def country_content_stats():

    stats = db.session.query(
        Country.name.label('country_name'),
        func.count(NetflixContent.show_id).label('content_count')
    ).join(
        NetflixContent, NetflixContent.country_id == Country.identifier
    ).group_by(
        Country.name
    ).all()

    if not stats:
        return jsonify({
            'min_content_count': 0,
            'max_content_count': 0,
            'avg_content_count': 0,
            'total_countries': 0
        })

    content_counts = [count for _, count in stats]
    
    return jsonify({
        'min_content_count': min(content_counts),
        'max_content_count': max(content_counts),
        'avg_content_count': float(sum(content_counts)) / len(content_counts),
        'total_countries': len(stats),
        'details': [{'country': country_name, 'content_count': count} for country_name, count in stats]
    })


# curl -i http://127.0.0.1:5000/api/stats/release-year-content-stats
@app.route('/api/stats/release-year-content-stats', methods=['GET'])
def release_year_content_stats():

    stats = db.session.query(
        NetflixContent.release_year.label('release_year'),
        func.count(NetflixContent.show_id).label('content_count')
    ).filter(
        NetflixContent.release_year.isnot(None)
    ).group_by(
        NetflixContent.release_year
    ).order_by(
        NetflixContent.release_year.desc()
    ).all()

    if not stats:
        return jsonify({
            'min_content_count': 0,
            'max_content_count': 0,
            'avg_content_count': 0,
            'total_years': 0
        })

    content_counts = [count for _, count in stats]
    
    return jsonify({
        'min_content_count': min(content_counts),
        'max_content_count': max(content_counts),
        'avg_content_count': float(sum(content_counts)) / len(content_counts),
        'total_years': len(stats),
        'details': [{'release_year': year, 'content_count': count} for year, count in stats]
    })


# Все виды контента
# curl -i http://127.0.0.1:5000/api/content-types

# Все страны
# curl -i http://127.0.0.1:5000/api/countries

# Все рейтинги
# curl -i http://127.0.0.1:5000/api/ratings

# Весь Netflix-контент
# curl -i http://127.0.0.1:5000/api/content

# Для каждого года выпуска: min, max и avg длительности фильмов (в минутах)
# curl -i http://127.0.0.1:5000/api/stats/min-max-avg-duration

# Статистика по рейтингам: мин, макс, среднее количество контента
# curl -i http://127.0.0.1:5000/api/stats/rating-content-stats

# Статистика по странам: мин, макс, среднее количество контента
# curl -i http://127.0.0.1:5000/api/stats/country-content-stats

# Статистика по годам выпуска: мин, макс, среднее количество контента
# curl -i http://127.0.0.1:5000/api/stats/release-year-content-stats


# примеры взаимодействия
# curl -i -H "Content-Type: application/json" -X POST http://127.0.0.1:5000/api/content -d "{\"show_id\": \"test001\", \"title\": \"Test Movie 1\", \"type_id\": 1, \"director\": \"Test Director\", \"cast\": \"Test Actor 1, Test Actor 2\", \"country_id\": 1, \"release_year\": 2024, \"rating_id\": 1, \"duration_minutes\": 120, \"duration_seasons\": null}"

# curl -i -H "Content-Type: application/json" -X PUT http://127.0.0.1:5000/api/content/test001 -d "{\"type_id\": 2"}"

# curl -i -X DELETE http://127.0.0.1:5000/api/content/test001

# curl -i http://127.0.0.1:5000/api/content/test001

# Обработчик ошибок 404
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({
        'error': 'Not Found',
        'message': 'Запрашиваемый ресурс не найден',
        'status': 404
    }), 404

# Обработчик ошибок 400
@app.errorhandler(400)
def bad_request_error(error):
    return jsonify({
        'error': 'Bad Request',
        'message': 'Неверный формат запроса',
        'status': 400
    }), 400

# Обработчик ошибок 500
@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'error': 'Internal Server Error',
        'message': 'Внутренняя ошибка сервера',
        'status': 500
    }), 500

# Обработчик общих исключений
@app.errorhandler(Exception)
def handle_exception(error):
    # Логируем ошибку
    app.logger.error(f"Необработанное исключение: {str(error)}")

    return jsonify({
        'error': 'Internal Server Error',
        'message': 'Произошла непредвиденная ошибка',
        'status': 500
    }), 500