"""CLI do barramento. O padrão é a implementação em memória."""

from __future__ import annotations

import argparse
import json
import sys

from bus import InMemoryBus, KafkaBus
from orders import gerar_pedidos_demo, processar_pedidos, publicar_pedidos

_BARRAMENTO = None


def obter_barramento(backend: str, bootstrap: str):
    global _BARRAMENTO
    if backend == "memory":
        if _BARRAMENTO is None or not isinstance(_BARRAMENTO, InMemoryBus):
            _BARRAMENTO = InMemoryBus()
        return _BARRAMENTO
    if backend == "kafka":
        return KafkaBus(bootstrap)
    raise ValueError("Backend deve ser memory ou kafka.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Demonstra eventos de pedidos em memória ou Kafka.")
    parser.add_argument("--backend", choices=("memory", "kafka"), default="memory")
    parser.add_argument("--bootstrap", default="localhost:9092")
    sub = parser.add_subparsers(dest="comando", required=True)

    publicar = sub.add_parser("publicar")
    publicar.add_argument("--topic", required=True)
    publicar.add_argument("--key", required=True)
    publicar.add_argument("--valor", default="{}")

    gerar = sub.add_parser("gerar-demo", help="Publica pedidos de demonstração no tópico pedidos.")
    gerar.add_argument("--quantidade", type=int, default=3)

    processar = sub.add_parser("processar-pedidos", help="Valida pedidos, atualiza indicadores e envia rejeitados.")
    processar.add_argument("--grupo", default="analytics")
    processar.add_argument("--limite", type=int, default=100)

    demo = sub.add_parser("demo", help="Gera e processa pedidos no mesmo processo.")
    demo.add_argument("--quantidade", type=int, default=3)
    demo.add_argument("--grupo", default="analytics")

    consumir = sub.add_parser("consumir")
    consumir.add_argument("--topic", required=True)
    consumir.add_argument("--grupo", default="analytics")
    consumir.add_argument("--commit", action="store_true")
    consumir.add_argument("--limite", type=int, default=100)

    retroceder = sub.add_parser("replay")
    retroceder.add_argument("--topic", required=True)
    retroceder.add_argument("--grupo", default="analytics")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        barramento = obter_barramento(args.backend, args.bootstrap)
        if args.comando == "publicar":
            barramento.publish(args.topic, args.key, json.loads(args.valor))
            print(json.dumps({"topic": args.topic, "key": args.key}, ensure_ascii=False))
        elif args.comando == "gerar-demo":
            pedidos = gerar_pedidos_demo(args.quantidade)
            publicar_pedidos(barramento, pedidos)
            print(json.dumps({"topic": "pedidos", "publicados": pedidos}, ensure_ascii=False, indent=2))
        elif args.comando == "processar-pedidos":
            resultado = processar_pedidos(barramento, args.grupo, args.limite)
            print(json.dumps({
                "recebidos": resultado.recebidos,
                "validos": resultado.validos,
                "invalidos": resultado.invalidos,
                "offset_confirmado": resultado.offset_confirmado,
                "indicadores": {
                    "quantidade_pedidos": resultado.indicadores.quantidade_pedidos,
                    "valor_total": resultado.indicadores.valor_total,
                },
            }, ensure_ascii=False, indent=2))
        elif args.comando == "demo":
            pedidos = gerar_pedidos_demo(args.quantidade)
            publicar_pedidos(barramento, pedidos)
            resultado = processar_pedidos(barramento, args.grupo)
            print(json.dumps({
                "publicados": len(pedidos),
                "recebidos": resultado.recebidos,
                "validos": resultado.validos,
                "invalidos": resultado.invalidos,
                "offset_confirmado": resultado.offset_confirmado,
                "indicadores": {
                    "quantidade_pedidos": resultado.indicadores.quantidade_pedidos,
                    "valor_total": resultado.indicadores.valor_total,
                },
            }, ensure_ascii=False, indent=2))
        elif args.comando == "replay":
            barramento.rewind(args.topic, args.grupo)
            print(json.dumps({"topic": args.topic, "grupo": args.grupo, "offset": 0}, ensure_ascii=False))
        else:
            registros = barramento.consume(args.topic, args.grupo, args.limite)
            offset = barramento.commit(args.topic, args.grupo) if args.commit else None
            print(json.dumps({"eventos": registros, "offset_confirmado": offset}, ensure_ascii=False, indent=2))
        if args.backend == "kafka":
            barramento.close()
        return 0
    except Exception as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
