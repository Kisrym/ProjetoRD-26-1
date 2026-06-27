# ProjetoRD-26-1

Projeto de comunicação P2P desenvolvido em Python para a disciplina de Redes. A aplicação permite que peers se registrem em um servidor Rendezvous, descubram outros peers ativos, estabeleçam conexões TCP diretas e troquem mensagens diretas ou publicações em grupo.

O projeto pode ser usado de duas formas:

- Com interface web, que é o modo padrão.
- Apenas pelo terminal, usando a flag `--cli`.

## Como o projeto funciona

A aplicação utiliza um servidor Rendezvous central com comunicação direta entre peers. O Rendezvous não transporta as mensagens dos usuários; ele serve como ponto de encontro para registro e descoberta. Depois que um peer descobre outros peers, a comunicação acontece diretamente por sockets TCP.

O fluxo principal é:

1. O peer lê as configurações em `config.json`.
2. O peer se registra no Rendezvous usando nome, namespace, porta e TTL.
3. Periodicamente, o peer renova seu registro antes do TTL expirar.
4. Também periodicamente, o peer consulta o Rendezvous para descobrir outros peers.
5. Ao encontrar peers, a aplicação tenta abrir uma conexão direta e faz um handshake com mensagens `HELLO` e `HELLO_OK`.
6. Depois da conexão estabelecida, o roteador processa mensagens `SEND`, `ACK`, `PUB`, `PING`, `PONG`, `BYE` e `BYE_OK`.
7. Um mecanismo de keep-alive envia `PING` para conexões ociosas e tenta reconectar peers quando uma queda é detectada.

## Implementação

O projeto foi implementado com `asyncio`, usando tarefas concorrentes para manter vários serviços rodando ao mesmo tempo. O arquivo `main.py` inicia a aplicação e executa, em paralelo, o registro no Rendezvous, o servidor TCP local, a descoberta de peers, o roteamento de mensagens, o keep-alive e o loop de comandos.

Principais componentes:

- `main.py`: ponto de entrada da aplicação; decide se a execução será com webapp ou somente terminal.
- `config.py`: carrega os valores de `config.json` e expõe constantes usadas pelo restante do projeto.
- `core/rendezvous.py`: implementa `REGISTER`, renovação de registro, `UNREGISTER` e `DISCOVER`.
- `core/server.py`: abre o servidor TCP local do peer e coloca mensagens recebidas em uma fila assíncrona.
- `core/router.py`: consome a fila de mensagens e chama o handler correto para cada tipo de mensagem.
- `core/connection.py`: implementa keep-alive, reconexão e fechamento limpo das conexões.
- `core/peer_table.py`: mantém a tabela de peers conhecidos e o estado de cada conexão.
- `handlers/`: contém os handlers de protocolo, como handshake, ping/pong, envio direto, publicação e encerramento.
- `interfaces/cli.py`: implementa os comandos de terminal.
- `interfaces/web/app.py`: implementa a interface web com Quart e Socket.IO.

## Como rodar

### 1. Criar um ambiente virtual

Na raiz do projeto, crie e ative um ambiente virtual:

```bash
python -m venv .venv
```

No Linux/macOS:

```bash
source .venv/bin/activate
```

No Windows:

```bash
.venv\Scripts\activate
```

### 2. Instalar as dependências

Com o ambiente virtual ativado, instale as dependências listadas em `requirements.txt`:

```bash
pip install -r requirements.txt
```

Essa etapa instala as bibliotecas usadas pela aplicação, incluindo Quart, Hypercorn e Socket.IO.

### 3. Rodar no modo web, sem `--cli`

Este é o modo padrão:

```bash
python main.py
```

Ao iniciar sem `--cli`, o projeto sobe a interface web na porta configurada em `webapp_port`. Com a configuração padrão, acesse:

```text
http://localhost:8000
```

Nesse modo, o terminal é integrado ao webapp. A interface web solicita o nome e o namespace do peer antes de liberar o restante da execução. Depois disso, o peer se registra no Rendezvous, descobre outros peers e passa a aceitar comandos e mensagens.

### 4. Rodar no modo terminal, com `--cli`

Para executar sem interface web:

```bash
python main.py --cli
```

Nesse modo, o programa usa diretamente os valores `name` e `namespace` definidos em `config.json`:

```json
{
  "name": "user",
  "namespace": "test"
}
```

Antes de rodar com `--cli`, edite esses campos para identificar o peer corretamente. Tambem ajuste `peer_port` se outro peer já estiver usando a mesma porta.

## Comandos disponiveis no terminal

Depois que o peer estiver rodando, alguns comandos podem ser usados pelo terminal ou pelo terminal principal integrado da interface web:

- `/peers`: lista os peers conhecidos.
- `/conn`: lista conexões inbound e outbound.
- `/msg peer@namespace mensagem`: envia uma mensagem direta para um peer conectado.
- `/pub #namespace mensagem`: publica uma mensagem para peers de um grupo/namespace.
- `/pub * mensagem`: publica uma mensagem para todos os peers conectados conhecidos.
- `/reconnect`: força tentativa de reconexão com peers conhecidos.
- `/quit`: encerra conexões, desregistra o peer no Rendezvous e finaliza o programa.

## Configurações

As configurações ficam em `config.json`:

- `host` e `port`: endereço e porta do servidor Rendezvous.
- `name`: nome usado pelo peer no modo `--cli`.
- `namespace`: namespace usado pelo peer no modo `--cli`.
- `peer_port`: porta TCP local usada para conexoes P2P.
- `ttl`: tempo de validade do registro no Rendezvous.
- `webapp_port`: porta usada pela interface web.
- `max_peer_reconnect_attempts`: número máximo de tentativas de reconexão com outro peer.
- `max_rdzv_reconnect_attempts`: número máximo de tentativas de registro no Rendezvous.
- `max_rdzv_discover_attempts`: número máximo de tentativas de descoberta de peers.
- `ping_interval_seconds`: intervalo de ociosidade antes de enviar `PING`.
