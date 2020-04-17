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
