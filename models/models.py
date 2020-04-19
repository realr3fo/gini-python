from main import db


class Logs(db.Model):
    __tablename__ = 'logs'

    id = db.Column(db.Integer, primary_key=True)
    entity = db.Column(db.String())
    properties = db.Column(db.String())
    timestamp = db.Column(db.String())

    def __init__(self, entity, properties, timestamp):
        self.entity = entity
        self.properties = properties
        self.timestamp = timestamp

    def __repr__(self):
        return '<id {}>'.format(self.id)

    def serialize(self):
        return {
            'id': self.id,
            'entity': self.entity,
            'properties': self.properties,
            'timestamp': self.timestamp
        }


class Dashboards(db.Model):
    __tablename__ = 'dashboards'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String())
    author = db.Column(db.String())
    entity = db.Column(db.String())
    hash_code = db.Column(db.String())
    filters = db.Column(db.String())
    properties = db.Column(db.String())
    timestamp = db.Column(db.String())

    def __init__(self, name, author, entity, hash_code, filters, properties, timestamp):
        self.name = name
        self.author = author
        self.entity = entity
        self.hash_code = hash_code
        self.filters = filters
        self.properties = properties
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
            'hash_code': self.hash_code,
            'filters': self.filters,
            'properties': self.properties,
            'timestamp': self.timestamp
        }
