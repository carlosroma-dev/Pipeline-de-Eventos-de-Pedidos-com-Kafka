# Processamento de eventos de pedidos com Kafka

Neste projeto, desenvolvi uma demonstração de engenharia de dados para processar pedidos de e-commerce. Um produtor publica eventos JSON, um consumidor valida os dados, calcula indicadores e direciona eventos inválidos para um tópico de rejeitados.

## O que o projeto faz

- Publica eventos no tópico `pedidos`.
- Valida os campos `event_id`, `tipo`, `pedido_id`, `valor` e `criado_em`.
- Calcula a quantidade de pedidos válidos e o valor total processado.
- Publica eventos inválidos no tópico `pedidos-invalidos`, junto com o motivo da rejeição.
- Permite executar o mesmo fluxo em memória ou com Kafka local.

Um evento de pedido tem este formato:

```json
{
  "event_id": "9b53fa1e-3c23-456c-a516-6a3c9579ad04",
  "tipo": "pedido_criado",
  "pedido_id": "PED-001",
  "valor": 149.9,
  "criado_em": "2026-10-16T12:00:00+00:00"
}
```

O tipo esperado é `pedido_criado`; os identificadores precisam ser textos não vazios; o valor precisa ser numérico, finito e não negativo; e a data precisa seguir ISO 8601.

## Tecnologias

- Python 3.11+
- Apache Kafka com KRaft, executado pelo Docker Compose
- `kafka-python` para produzir e consumir mensagens
- `pytest` para os testes automatizados

## Como executar em memória

Instale as dependências:

```powershell
py -m pip install -r requirements.txt
```

Gere e processe três pedidos em uma execução:

```powershell
py src\main.py --backend memory demo
```

O resultado mostra os eventos processados, a quantidade válida e o valor total. A fila em memória existe apenas durante a execução do processo.

## Como executar com Kafka

Com o Docker Desktop aberto, inicie o broker na pasta do projeto:

```powershell
docker compose up -d
docker compose ps
```

Gere pedidos e processe-os usando o mesmo grupo de consumidores:

```powershell
py src\main.py --backend kafka --bootstrap localhost:9092 gerar-demo --quantidade 3
py src\main.py --backend kafka --bootstrap localhost:9092 processar-pedidos --grupo linkedin-demo
```

Também posso publicar um evento inválido para conferir o fluxo de rejeição. No PowerShell, uso as barras antes das aspas do JSON para que elas cheguem corretamente ao programa:

```powershell
py src\main.py --backend kafka --bootstrap localhost:9092 publicar --topic pedidos --key PED-INVALIDO --valor '{\"event_id\":\"evt-invalido\",\"tipo\":\"pedido_criado\",\"pedido_id\":\"PED-INVALIDO\",\"valor\":\"abc\",\"criado_em\":\"2026-10-16T12:00:00Z\"}'
py src\main.py --backend kafka --bootstrap localhost:9092 processar-pedidos --grupo linkedin-demo
py src\main.py --backend kafka --bootstrap localhost:9092 consumir --topic pedidos-invalidos --grupo inspecao-linkedin --commit
```

O valor textual `abc` é inválido para `valor`. O consumidor deve encaminhar a mensagem para `pedidos-invalidos` com o motivo da rejeição. Para encerrar o broker:

```powershell
docker compose down
```

## Testes

```powershell
py -m pytest -q
```

Os testes usam o barramento em memória e cobrem validação, indicadores, envio de rejeitados, offsets e falhas durante o processamento.

## Como organizei o fluxo

Defini uma interface comum para o barramento e implementei duas opções: `InMemoryBus`, usada para testar regras sem depender de infraestrutura, e `KafkaBus`, que usa o broker real. Assim, as regras do pedido são as mesmas nos dois caminhos.

O consumidor trabalha com commit manual. Ele publica todas as rejeições do lote e, depois, confirma a posição seguinte às mensagens lidas. Se ocorrer uma falha, a tentativa seguinte pode retomar do último commit. Os indicadores são mantidos em memória para deixar a demonstração simples.

## Validação e limitações

Executei os 10 testes automatizados e a demonstração em memória. Também validei localmente com Kafka a publicação e o consumo de três pedidos válidos: o consumidor calculou o total de `459.4` e confirmou a posição `6` no tópico. O fluxo de rejeitados está coberto pelos testes em memória; para conferir esse caminho no Kafka, os comandos acima publicam um pedido inválido e leem a DLQ.

Este projeto é uma demonstração de portfólio. Os indicadores não são persistidos, então reiniciar o processo não mantém o total. A publicação da rejeição e o commit do pedido são operações separadas, então uma falha entre as duas pode gerar uma rejeição duplicada numa nova tentativa. O broker local do Compose não tem autenticação nem TLS.
