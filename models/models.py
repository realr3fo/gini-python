from main import db


class Dashboards(db.Model):
    __tablename__ = 'dashboards'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String())
    author = db.Column(db.String())
    entity = db.Column(db.String())
    hash_code = db.Column(db.String(), unique=True)
    filters = db.Column(db.String(), default="[]")
    properties = db.Column(db.String(), default="[]")
    timestamp = db.Column(db.String())
    # instances = db.Column(db.JSON(), default={})
    compare_filters = db.Column(db.String(), default="[]")
    analysis_filters = db.Column(db.String(), default="[]")
    additional_filters = db.Column(db.String(), default="[]")
    entity_info = db.Column(db.JSON(), default={})
    filters_info = db.Column(db.JSON(), default={})
    properties_info = db.Column(db.JSON(), default={})
    analysis_info = db.Column(db.JSON(), default={})
    public = db.Column(db.Boolean, default=True)

    def __init__(self, name, author, entity, hash_code, timestamp):
        self.name = name
        self.author = author
        self.entity = entity
        self.hash_code = hash_code
        self.timestamp = timestamp

    def __repr__(self):
        return """{
            'id': %s,
            'name': %s,
            'author': %s,
            'entity': %s,
            'hash_code': %s,
            'filters': %s,
            'properties': %s,
            'timestamp': %s
        }""" % (
            self.id, self.name, self.author, self.entity, self.hash_code, self.filters, self.properties, self.timestamp)

    def serialize(self):
        return {
            'id': self.id,
            'name': self.name,
            'author': self.author,
            'entity': self.entity,
            'hashCode': self.hash_code,
            'filters': self.filters,
            'properties': self.properties,
            'timestamp': self.timestamp,
            # 'instances': self.instances,
            'compareFilters': self.compare_filters,
            'analysisFilters': self.analysis_filters,
            'additionalFilters': self.additional_filters,
            "entityInfo": self.entity_info,
            "filtersInfo": self.filters_info,
            "propertiesInfo": self.properties_info
        }


class Analysis(db.Model):
    __tablename__ = 'analysis'
    id = db.Column(db.Integer, primary_key=True)
    dashboard_id = db.Column(db.Integer, db.ForeignKey('dashboards.id'))
    filter_limit = db.Column(db.JSON, default={})
    shown_combinations = db.Column(db.JSON, default={})

    def __init__(self, dashboard_id):
        self.dashboard_id = dashboard_id
