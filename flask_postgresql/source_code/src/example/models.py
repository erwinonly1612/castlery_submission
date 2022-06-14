import flask_sqlalchemy

db = flask_sqlalchemy.SQLAlchemy()


class Employees(db.Model):
    __tablename__ = 'employees'
    id = db.Column(db.Integer, primary_key=True)
    employee = db.Column(db.String(100))
    gender = db.Column(db.String(1))
    age = db.Column(db.Integer)
    salary = db.Column(db.Float)
    town = db.Column(db.String(100))
