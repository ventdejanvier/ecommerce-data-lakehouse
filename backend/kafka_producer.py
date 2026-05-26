import json
import logging
import os
from functools import lru_cache
from typing import Any

from confluent_kafka import KafkaException, Producer

logger = logging.getLogger(__name__)


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)
        return

    logger.debug(
        "Kafka delivery succeeded: topic=%s partition=%s offset=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


@lru_cache(maxsize=1)
def _get_producer() -> Producer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return Producer({"bootstrap.servers": bootstrap_servers})


def produce_event(topic: str, event: dict[str, Any]) -> None:
    producer = _get_producer()
    payload = json.dumps(event, default=str, separators=(",", ":"))
    session_key = str(event.get("sessionId", ""))

    try:
        producer.produce(
            topic=topic,
            key=session_key.encode("utf-8") if session_key else None,
            value=payload.encode("utf-8"),
            callback=_delivery_report,
        )
        # Non-blocking poll to serve delivery callbacks.
        producer.poll(0)
        pending = producer.flush(
            float(os.getenv("KAFKA_FLUSH_TIMEOUT_SECONDS", "1.0"))
        )
        if pending:
            logger.warning(
                "Kafka producer still has %s pending message(s) for topic '%s'",
                pending,
                topic,
            )
    except BufferError:
        logger.warning(
            "Kafka producer queue is full; dropping event for topic '%s'", topic
        )
        producer.poll(0)
    except (KafkaException, TypeError, ValueError):
        logger.exception("Unexpected Kafka produce error for topic '%s'", topic)
