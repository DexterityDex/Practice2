from models import ContentType, Country, Rating, NetflixContent, db, ma


class ContentTypeSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = ContentType
        load_instance = True

    _links = ma.Hyperlinks(
        {
            "self": ma.URLFor("get_content_type", values=dict(id="<identifier>")),
            "collection": ma.URLFor("get_content_types"),
        }
    )


class CountrySchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Country
        load_instance = True

    _links = ma.Hyperlinks(
        {
            "self": ma.URLFor("get_country", values=dict(id="<identifier>")),
            "collection": ma.URLFor("get_countries"),
        }
    )


class RatingSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Rating
        load_instance = True

    _links = ma.Hyperlinks(
        {
            "self": ma.URLFor("get_rating", values=dict(id="<identifier>")),
            "collection": ma.URLFor("get_ratings"),
        }
    )


class NetflixContentSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = NetflixContent
        load_instance = True
        sqla_session = db.session

    type_rel = ma.Nested(ContentTypeSchema())
    country_rel = ma.Nested(CountrySchema())
    rating_rel = ma.Nested(RatingSchema())
    type_id = ma.auto_field()
    country_id = ma.auto_field()
    rating_id = ma.auto_field()

    _links = ma.Hyperlinks(
        {
            "self": ma.URLFor("get_content", values=dict(show_id="<show_id>")),
            "collection": ma.URLFor("get_all_content"),
        }
    )


content_type_schema = ContentTypeSchema()
content_types_schema = ContentTypeSchema(many=True)
country_schema = CountrySchema()
countries_schema = CountrySchema(many=True)
rating_schema = RatingSchema()
ratings_schema = RatingSchema(many=True)
content_schema = NetflixContentSchema()
contents_schema = NetflixContentSchema(many=True)