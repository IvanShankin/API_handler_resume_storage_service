import socket
from typing import Optional
from aiokafka.admin import AIOKafkaAdminClient

from src.service.config import get_config

_admin_client: Optional[AIOKafkaAdminClient] = None


async def init_admin_client() -> AIOKafkaAdminClient:
    global _admin_client
    if _admin_client is None:
        _admin_client = AIOKafkaAdminClient(
            bootstrap_servers=get_config().env.kafka_bootstrap_servers,
            client_id=socket.gethostname(),
        )
        await _admin_client.start()

    return _admin_client


async def set_admin_client(admin: AIOKafkaAdminClient):
    global _admin_client
    _admin_client = admin


async def get_admin_client() -> AIOKafkaAdminClient:
    global _admin_client
    if _admin_client is None:
        raise RuntimeError("Admin client not initialized")
    return _admin_client


async def shutdown_admin_client():
    global _admin_client
    if _admin_client:
        await _admin_client.close()
        _admin_client = None
