"""Barramento de eventos: memória para teste e Kafka real atrás da mesma interface."""

from __future__ import annotations

import json
from typing import Any, Protocol


class EventBus(Protocol):
    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None: ...

    def consume(self, topic: str, group: str, limit: int = 100) -> list[dict[str, Any]]: ...

    def commit(self, topic: str, group: str) -> int: ...

    def rewind(self, topic: str, group: str) -> None: ...

    def rollback(self, topic: str, group: str) -> None: ...

    def close(self) -> None: ...


class InMemoryBus:
    """Fila em memória com offset por grupo. Usada pelos testes, sem broker."""

    def __init__(self) -> None:
        self._topics: dict[str, list[dict[str, Any]]] = {}
        self._offsets: dict[tuple[str, str], int] = {}
        self._pendente: dict[tuple[str, str], int] = {}

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        log = self._topics.setdefault(topic, [])
        log.append({"offset": len(log), "key": key, "value": value})

    def consume(self, topic: str, group: str, limit: int = 100) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        inicio = self._offsets.get((topic, group), 0)
        lote = self._topics.get(topic, [])[inicio : inicio + limit]
        self._pendente[(topic, group)] = inicio + len(lote)
        return lote

    def commit(self, topic: str, group: str) -> int:
        chave = (topic, group)
        if chave not in self._pendente:
            raise RuntimeError("Não há lote para confirmar.")
        self._offsets[chave] = self._pendente.pop(chave)
        return self._offsets[chave]

    def rewind(self, topic: str, group: str) -> None:
        self._offsets[(topic, group)] = 0
        self._pendente.pop((topic, group), None)

    def rollback(self, topic: str, group: str) -> None:
        self._pendente.pop((topic, group), None)

    def close(self) -> None:
        return None


class KafkaBus:
    """Produtor e consumidor kafka-python. Não é exercitado sem um broker no ar."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer = None
        self._consumers: dict[tuple[str, str], Any] = {}

    def _produtor(self):
        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                key_serializer=lambda chave: chave.encode("utf-8") if chave else None,
                value_serializer=lambda valor: json.dumps(valor, ensure_ascii=False).encode("utf-8"),
                acks="all",
            )
        return self._producer

    def _consumidor(self, topic: str, group: str):
        chave = (topic, group)
        if chave not in self._consumers:
            from kafka import KafkaConsumer

            self._consumers[chave] = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                consumer_timeout_ms=2000,
                key_deserializer=lambda item: item.decode("utf-8") if item else None,
                value_deserializer=lambda item: json.loads(item.decode("utf-8")),
            )
        return self._consumers[chave]

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        futuro = self._produtor().send(topic, key=key, value=value)
        futuro.get(timeout=10)

    def consume(self, topic: str, group: str, limit: int = 100) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        consumidor = self._consumidor(topic, group)
        registros = []
        for mensagem in consumidor:
            registros.append(
                {
                    "offset": mensagem.offset,
                    "key": mensagem.key,
                    "value": mensagem.value,
                }
            )
            if len(registros) >= limit:
                break
        return registros

    def commit(self, topic: str, group: str) -> int:
        consumidor = self._consumidor(topic, group)
        posicoes = [
            posicao
            for particao in consumidor.assignment()
            if (posicao := consumidor.position(particao)) is not None
        ]
        consumidor.commit()
        return max(posicoes, default=0)

    def rewind(self, topic: str, group: str) -> None:
        consumidor = self._consumidor(topic, group)
        consumidor.poll(timeout_ms=0)
        consumidor.seek_to_beginning()
        consumidor.commit()

    def rollback(self, topic: str, group: str) -> None:
        consumidor = self._consumers.pop((topic, group), None)
        if consumidor is not None:
            # Fechar força a próxima leitura do grupo a retomar do último commit.
            consumidor.close()

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
            self._producer = None
        for consumidor in self._consumers.values():
            consumidor.close()
        self._consumers.clear()
