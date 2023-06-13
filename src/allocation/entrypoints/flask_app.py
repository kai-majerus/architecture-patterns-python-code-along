import os
import datetime
import waitress
from typing import TypeVar
from flask import Blueprint, Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.allocation import config
from src.allocation.domain import model
from src.allocation.adapters import orm
from src.allocation.adapters import repository
from src.allocation.service_layer import services, unit_of_work


orm.start_mappers()
get_session = sessionmaker(bind=create_engine(config.get_postgres_uri()))
blueprint = Blueprint("get_campaigns", __name__)


# Allows create app to be called with arbitrary config objects
T = TypeVar("T")


def create_app() -> None:

    app = Flask(__name__)
    CORS(app)

    register_blueprints(app)

    waitress.serve(app, host="0.0.0.0", port=os.getenv("PORT", default=8080))


def register_blueprints(app: Flask) -> None:
    app.register_blueprint(blueprint)


@blueprint.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    line = model.OrderLine(
        request.json["orderid"],
        request.json["sku"],
        request.json["qty"],
    )
    try:
        uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory=session)
        batchref = services.allocate(line.orderid, line.sku, line.qty, uow)
    except (model.OutOfStock, services.InvalidSku) as e:
        return jsonify({"message": str(e)}), 400
    return jsonify({"batchref": batchref}), 201


@blueprint.route("/add_batch", methods=["POST"])
def add_batch():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    eta = request.json["eta"]
    if eta is not None:
        eta = datetime.datetime.fromisoformat(eta).date()
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    services.add_batch(
        request.json["ref"],
        request.json["sku"],
        request.json["qty"],
        eta,
        uow,
    )
    return "OK", 201
