"""Regras do caso de uso de pedidos de e-commerce."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any
from uuid import uuid4

from bus import EventBus

TOPICO_PEDIDOS = "pedidos"
TOPICO_INVALIDOS = "pedidos-invalidos"
TIPO_PEDIDO = "pedido_criado"


def gerar_pedidos_demo(quantidade: int = 3) -> list[dict[str, Any]]:
    """Cria pedidos válidos com IDs únicos para uma demonstração."""
    if quantidade < 1:
        raise ValueError("A quantidade precisa ser maior que zero.")

    agora = datetime.now(timezone.utc)
    return [
        {
            "event_id": str(uuid4()),
            "tipo": TIPO_PEDIDO,
            "pedido_id": f"PED-{uuid4().hex[:8].upper()}",
            "valor": valor,
            "criado_em": agora.isoformat(),
        }
        for valor in (149.90, 89.50, 220.00)[:quantidade]
    ] if quantidade <= 3 else _gerar_pedidos_adicionais(quantidade, agora)


def _gerar_pedidos_adicionais(quantidade: int, agora: datetime) -> list[dict[str, Any]]:
    valores = [149.90, 89.50, 220.00]
    return [
        {
            "event_id": str(uuid4()),
            "tipo": TIPO_PEDIDO,
            "pedido_id": f"PED-{uuid4().hex[:8].upper()}",
            "valor": valores[indice % len(valores)],
            "criado_em": agora.isoformat(),
        }
        for indice in range(quantidade)
    ]


def publicar_pedidos(bus: EventBus, pedidos: list[dict[str, Any]]) -> None:
    for pedido in pedidos:
        bus.publish(TOPICO_PEDIDOS, pedido["pedido_id"], pedido)


def validar_evento(evento: Any) -> str | None:
    """Retorna o motivo da rejeição, ou None quando o pedido é válido."""
    if not isinstance(evento, dict):
        return "O evento deve ser um objeto JSON."

    obrigatorios = ("event_id", "tipo", "pedido_id", "valor", "criado_em")
    ausentes = [campo for campo in obrigatorios if campo not in evento]
    if ausentes:
        return f"Campos obrigatórios ausentes: {', '.join(ausentes)}."

    for campo in ("event_id", "pedido_id", "tipo"):
        if not isinstance(evento[campo], str) or not evento[campo].strip():
            return f"O campo '{campo}' deve ser uma string não vazia."
    if evento["tipo"] != TIPO_PEDIDO:
        return f"O campo 'tipo' deve ser '{TIPO_PEDIDO}'."

    valor = evento["valor"]
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return "O campo 'valor' deve ser numérico."
    try:
        valor_finito = math.isfinite(valor)
    except OverflowError:
        valor_finito = False
    if not valor_finito or valor < 0:
        return "O campo 'valor' deve ser finito e não negativo."

    criado_em = evento["criado_em"]
    if not isinstance(criado_em, str):
        return "O campo 'criado_em' deve ser uma data ISO 8601 em texto."
    try:
        datetime.fromisoformat(criado_em.replace("Z", "+00:00"))
    except ValueError:
        return "O campo 'criado_em' deve conter uma data ISO 8601 válida."
    return None


@dataclass
class Indicadores:
    quantidade_pedidos: int = 0
    valor_total: float = 0.0


@dataclass
class ResultadoProcessamento:
    recebidos: int
    validos: int
    invalidos: int
    offset_confirmado: int
    indicadores: Indicadores


def processar_pedidos(
    bus: EventBus,
    grupo: str = "analytics",
    limite: int = 100,
    indicadores: Indicadores | None = None,
) -> ResultadoProcessamento:
    """Processa um lote, envia rejeitados e confirma após concluir o lote."""
    metricas = indicadores if indicadores is not None else Indicadores()
    registros = bus.consume(TOPICO_PEDIDOS, grupo, limite)
    quantidade_valida = 0
    valor_valido = 0.0
    quantidade_invalida = 0

    try:
        for registro in registros:
            evento = registro.get("value")
            motivo = validar_evento(evento)
            if motivo is not None:
                rejeitado = {"evento": evento, "motivo_rejeicao": motivo}
                bus.publish(TOPICO_INVALIDOS, registro.get("key") or "invalido", rejeitado)
                quantidade_invalida += 1
            else:
                quantidade_valida += 1
                valor_valido += float(evento["valor"])

        offset = bus.commit(TOPICO_PEDIDOS, grupo) if registros else 0
    except Exception:
        bus.rollback(TOPICO_PEDIDOS, grupo)
        raise
    metricas.quantidade_pedidos += quantidade_valida
    metricas.valor_total = round(metricas.valor_total + valor_valido, 2)
    return ResultadoProcessamento(
        recebidos=len(registros),
        validos=quantidade_valida,
        invalidos=quantidade_invalida,
        offset_confirmado=offset,
        indicadores=metricas,
    )
