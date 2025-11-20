from fastapi import Request
from models.domain import Cluster
from models.config import AppConfig
from services.peer_service import PeerService


def get_cluster(request: Request) -> Cluster:
    return request.app.state.cluster


def get_peer_service(request: Request) -> PeerService:
    return request.app.state.peer_service


def get_config(request: Request) -> AppConfig:
    return request.app.state.config
