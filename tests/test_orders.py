from datetime import datetime, timezone

import pytest

from bus import InMemoryBus
from orders import (
    Indicadores,
    TOPICO_INVALIDOS,
    TOPICO_PEDIDOS,
    processar_pedidos,
    validar_evento,
)


def evento_valido(**sobrescritos):
    evento = {
        "event_id": "evt-1",
        "tipo": "pedido_criado",
        "pedido_id": "PED-001",
        "valor": 42.50,
        "criado_em": datetime.now(timezone.utc).isoformat(),
    }
    evento.update(sobrescritos)
    return evento


def test_valida_evento_completo():
    assert validar_evento(evento_valido()) is None


@pytest.mark.parametrize(
    ("alteracoes", "trecho_motivo"),
    [
        ({"valor": "42.50"}, "numérico"),
        ({"valor": True}, "numérico"),
        ({"pedido_id": ""}, "pedido_id"),
        ({"criado_em": "ontem"}, "ISO 8601"),
        ({"tipo": "pagamento"}, "tipo"),
    ],
)
def test_rejeita_campos_invalidos(alteracoes, trecho_motivo):
    motivo = validar_evento(evento_valido(**alteracoes))
    assert motivo is not None
    assert trecho_motivo in motivo


def test_processamento_atualiza_indicadores_e_confirma_offset():
    bus = InMemoryBus()
    bus.publish(TOPICO_PEDIDOS, "PED-001", evento_valido(valor=42.50))
    bus.publish(TOPICO_PEDIDOS, "PED-002", evento_valido(event_id="evt-2", pedido_id="PED-002", valor=7.25))
    indicadores = Indicadores()

    resultado = processar_pedidos(bus, indicadores=indicadores)

    assert (resultado.recebidos, resultado.validos, resultado.invalidos) == (2, 2, 0)
    assert indicadores.quantidade_pedidos == 2
    assert indicadores.valor_total == 49.75
    assert resultado.offset_confirmado == 2
    assert bus.consume(TOPICO_PEDIDOS, "analytics") == []


def test_evento_invalido_vai_para_topico_rejeitados_com_motivo():
    bus = InMemoryBus()
    evento = evento_valido(valor="42.50")
    bus.publish(TOPICO_PEDIDOS, "PED-001", evento)

    resultado = processar_pedidos(bus)
    rejeitados = bus.consume(TOPICO_INVALIDOS, "inspecao")

    assert resultado.invalidos == 1
    assert resultado.validos == 0
    assert len(rejeitados) == 1
    assert rejeitados[0]["value"]["evento"] == evento
    assert "numérico" in rejeitados[0]["value"]["motivo_rejeicao"]
    assert resultado.offset_confirmado == 1


def test_falha_ao_publicar_rejeitado_nao_confirma_nem_muda_indicadores():
    class BusComFalha(InMemoryBus):
        def publish(self, topic, key, value):
            if topic == TOPICO_INVALIDOS:
                raise RuntimeError("falha simulada no tópico de rejeitados")
            super().publish(topic, key, value)

    bus = BusComFalha()
    bus.publish(TOPICO_PEDIDOS, "PED-001", evento_valido(valor="inválido"))
    indicadores = Indicadores()

    with pytest.raises(RuntimeError, match="falha simulada"):
        processar_pedidos(bus, indicadores=indicadores)

    assert indicadores.quantidade_pedidos == 0
    assert bus.consume(TOPICO_PEDIDOS, "analytics")


def test_limit_zero_nao_consume_mensagem():
    bus = InMemoryBus()
    bus.publish(TOPICO_PEDIDOS, "PED-001", evento_valido())
    assert bus.consume(TOPICO_PEDIDOS, "analytics", 0) == []
