# Exclusão Mútua Centralizada em Sistemas Distribuídos

**Disciplina:** Sistemas Distribuídos — CEFET  
**Integrantes:** \_\_\_\_\_\_\_\_\_\_\_\_, \_\_\_\_\_\_\_\_\_\_\_\_, \_\_\_\_\_\_\_\_\_\_\_\_  
**Linguagem:** Python 3.12 (somente biblioteca padrão: `socket`, `threading`, `subprocess`, `logging`)

---

## 1. Objetivo e visão geral

O trabalho implementa o **algoritmo centralizado de exclusão mútua distribuída**: um processo
*coordenador* concede o acesso à região crítica (RC) a *n* processos clientes, um de cada vez, na
ordem de chegada dos pedidos. Cada cliente repete *r* vezes o ciclo
`REQUEST → GRANT → escreve em resultado.txt → sleep(k) → RELEASE`.

Por exigência do professor, a sincronização do algoritmo foi construída sobre o **monitor
`bounded_buffer`** do pseudocódigo da disciplina (`put_resource` / `get_resource` com as condições
`not_full` e `not_empty`). Ele é usado duas vezes no coordenador: como a **fila de pedidos** e como o
**canal de liberações** que faz a thread do algoritmo esperar o `RELEASE` do detentor.

| Arquivo | Responsabilidade |
|---|---|
| `src/mensagens.py` | Protocolo: tipos, codificação/decodificação em F bytes, leitura exata do socket |
| `src/monitor.py` | Monitor `BoundedBuffer` (tradução direta do pseudocódigo) |
| `src/coordenador.py` | Coordenador multi-thread (conexões, receptoras, algoritmo, interface) e log |
| `src/processo.py` | Processo cliente |
| `src/executar.py` | Sobe coordenador + *n* processos em sequência e roda os estudos de caso |
| `src/verificar.py` | Validação automática de `resultado.txt` e do log do coordenador |

## 2. Arquitetura

```
                       ┌─────────────────────────── Coordenador ───────────────────────────┐
 Processo 1 ──TCP──┐   │  [aceitar] ──cria──► [receptora P1] ─┐                              │
 Processo 2 ──TCP──┼──►│                      [receptora P2] ─┼─REQUEST─► pedidos (monitor)  │
     ...           │   │                      [receptora Pn] ─┘              │ get_resource   │
 Processo n ──TCP──┘   │                            │ RELEASE                 ▼                │
        ▲              │                            └──────► liberacoes ◄── [algoritmo] ──GRANT──► Pi
        │              │                                     (monitor)   get_resource          │
        └── GRANT ─────│  [interface] ── lê fila/contadores (sincronizado) ── stdin            │
                       └────────────────────────────────────────────────────────────────────┘
```

**Decisão — TCP com um socket por processo.** O enunciado permite TCP ou UDP. Escolhemos TCP por
garantir entrega e ordem das mensagens (o algoritmo centralizado assume canais confiáveis) e porque o
fim da conexão permite detectar a queda de um processo (seção 6). O coordenador mantém o dicionário
`_conexoes: dict[pid, socket]` para enviar o `GRANT` ao socket correto.

## 3. Protocolo de mensagens

Todas as mensagens têm tamanho fixo **F = 10 bytes** em ASCII, com o separador `|`:

```
<id do tipo>|<id do processo>|<preenchimento com '0' até F bytes>
REQUEST de P3 → 1|3|000000      GRANT para P3 → 2|3|000000      RELEASE de P3 → 3|3|000000
```

* Tipos (`Tipo(IntEnum)`): `REQUEST = 1`, `GRANT = 2`, `RELEASE = 3`.
* O pid é limitado a 6 dígitos (`PID_MAXIMO`) para sempre caber em F bytes com ao menos um zero de
  preenchimento, que marca o fim da mensagem.
* **Leitura exata (`receber`)**: TCP é um fluxo de bytes e `recv` pode devolver mensagens partidas ou
  coladas; por isso lemos em laço até completar F bytes. Tamanho fixo elimina a necessidade de
  delimitar mensagens no fluxo.
* **Validação defensiva (`Mensagem.decodificar`)**: rejeita mensagens com tamanho ≠ F, não-ASCII,
  número de campos diferente de 3, campos não numéricos, tipo inexistente ou preenchimento diferente de
  zeros. Uma mensagem inválida encerra a conexão do remetente e é registrada no log.

## 4. O monitor `bounded_buffer` (pseudocódigo → código)

O monitor foi implementado em `monitor.py` como uma classe genérica `BoundedBuffer[T]`, mantendo a
mesma estrutura do pseudocódigo:

| Pseudocódigo | Implementação em Python |
|---|---|
| `Monitor bounded_buffer` (exclusão mútua implícita) | `self._monitor = Lock()`; todo método entra com `with self._monitor` |
| `Resource buffer[N]` | `self._buffer: list[T \| None] = [None] * n` |
| `// Variables for indexing buffer` | `_entrada`, `_saida`, `_quantidade` (buffer circular) |
| `Condition not_full, not_empty` | `Condition(self._monitor)` — ambas sobre o **mesmo** lock |
| `while (buffer is full) wait(not_full)` | `while self._quantidade == self._n: self._not_full.wait()` |
| `signal(not_empty)` / `signal(not_full)` | `self._not_empty.notify()` / `self._not_full.notify()` |

```python
def put_resource(self, recurso: T) -> None:
    with self._monitor:
        while self._quantidade == self._n:
            self._not_full.wait()
        self._buffer[self._entrada] = recurso
        self._entrada = (self._entrada + 1) % self._n
        self._quantidade += 1
        self._not_empty.notify()
```

**Decisões:**

* **As condições compartilham o lock do monitor.** Isso reproduz a semântica de monitor: `wait()`
  libera o monitor atomicamente e o readquire ao acordar.
* **`while` e não `if`.** As condições do Python têm semântica de *Mesa*: quem é sinalizado não ganha o
  monitor de imediato e outra thread pode alterar o buffer antes. O `while` do pseudocódigo é,
  portanto, obrigatório para a corretude.
* **Buffer circular.** Os índices de entrada e saída andam módulo N, o que dá ordem **FIFO** em O(1).
  Essa ordem é a que garante o atendimento por ordem de chegada no algoritmo centralizado.
* **Único método extra: `snapshot()`.** A interface precisa imprimir a fila; o método copia o
  conteúdo em ordem FIFO **dentro do monitor**, sem expor a estrutura interna.

## 5. Coordenador

### 5.1 Threads

O enunciado pede um coordenador multi-thread, e o projeto segue a sugestão dele:

1. **`aceitar`** — fica bloqueada em `accept()`; para cada conexão nova cria uma **thread receptora**.
2. **Receptoras (uma por processo)** — bloqueiam em `receber()` no socket do processo. Na primeira
   mensagem associam o pid à conexão (pid duplicado é recusado). `REQUEST` vira
   `pedidos.put_resource(pid)`; `RELEASE` vira `liberacoes.put_resource(pid)`.
3. **`algoritmo`** — executa o algoritmo de exclusão mútua (abaixo).
4. **`interface`** — bloqueada lendo o terminal: `1` imprime o detentor atual e a fila de pedidos,
   `2` imprime quantas vezes cada processo foi atendido, `3` encerra.

**Decisão — uma thread receptora por socket** em vez de `select`: o código fica sequencial e simples
(cada thread lê só o seu socket) e não exige que a thread de conexões mexa em estruturas de outra
thread. Com dezenas de processos o custo de threads é irrelevante.

### 5.2 Algoritmo de exclusão mútua

```python
def algoritmo(self) -> None:
    while True:
        pid = self._pedidos.get_resource()          # bloqueia enquanto a fila está vazia
        with self._estado:
            conexao = self._conexoes.get(pid)
            if conexao is None:
                continue                            # processo caiu enquanto esperava
            self._detentor = pid
            self._atendimentos[pid] += 1
        grant = Mensagem(Tipo.GRANT, pid)
        self._registrar("SEND", grant)              # log ANTES do envio
        enviar(conexao, grant)
        self._liberacoes.get_resource()             # bloqueia até o RELEASE do detentor
```

O algoritmo virou um **produtor–consumidor**: as receptoras produzem pedidos e a thread do algoritmo
os consome um por vez. A RC só é concedida ao próximo depois que `liberacoes.get_resource()` retorna,
então **nunca há dois GRANTs pendentes**. Isso garante a exclusão mútua por construção, sem precisar de
uma flag "RC ocupada" separada. A fila do monitor é FIFO, então o atendimento segue a ordem de chegada
e **não há inanição**: cada processo espera no máximo n−1 acessos à frente dele.

**Decisão — capacidade N = 256.** Cada processo tem no máximo um pedido pendente (só pede de novo
depois do `RELEASE`), então a fila nunca passa de *n*. N só precisa ser ≥ *n*; com N menor,
`put_resource` bloquearia a receptora em `not_full`, o que continua correto, só mais lento.

### 5.3 Sincronização das estruturas compartilhadas

| Estrutura | Acessada por | Proteção |
|---|---|---|
| Fila `pedidos` | receptoras, algoritmo, interface | monitor `BoundedBuffer` |
| Canal `liberacoes` | receptoras, algoritmo | monitor `BoundedBuffer` |
| `_conexoes`, `_atendimentos`, `_detentor` | todas | `Lock` `_estado` |
| Ordem "log de REQUEST × entrada na fila" | receptoras | `Lock` `_ordem_pedidos` |
| Arquivo de log | todas | `logging` (handler já thread-safe) |

O lock `_ordem_pedidos` resolve um detalhe sutil. Se a receptora de P2 registrasse o `REQUEST` no log e
a de P3 entrasse na fila antes dela, o log mostraria uma ordem diferente da ordem real de atendimento.
Por isso o registro e o `put_resource` são feitos juntos, de forma atômica. Pelo mesmo motivo o `GRANT`
é registrado **antes** de ser enviado: se fosse depois, o processo poderia responder com `RELEASE` e
esse `RELEASE` apareceria no log antes do `GRANT`.

### 5.4 Log

O arquivo `coordenador.log` traz o instante em milissegundos, a direção, o tipo e o processo de
origem ou destino. Eventos anormais aparecem como linhas `INFO`:

```
2026-10-13 11:51:51.316 | RECV | REQUEST | P1
2026-10-13 11:51:51.316 | SEND | GRANT | P1
2026-10-13 11:51:51.319 | RECV | REQUEST | P2
2026-10-13 11:51:51.418 | RECV | RELEASE | P1
2026-10-13 11:51:51.418 | SEND | GRANT | P2
```

## 6. Robustez e tratamento de falhas

| Situação | Tratamento |
|---|---|
| Mensagem mal formada / tipo inválido | conexão encerrada, linha `INFO` no log |
| Segundo cliente com pid já conectado | conexão recusada |
| `RELEASE` de quem não detém a RC | ignorado (só o detentor libera) |
| Processo morre **dentro** da RC | a receptora detecta o fim do socket e força a liberação |
| Processo morre **na fila** | o algoritmo descarta o pedido ao não achar a conexão |

O tratamento de `RELEASE` e da desconexão consulta e zera `_detentor` dentro do mesmo lock. Assim, para
cada `GRANT` entra **exatamente uma** liberação no canal: ou o `RELEASE` legítimo, ou a liberação
forçada pela queda. Isso evita liberações "sobrando" que poderiam liberar a RC antes da hora mais
tarde. O teste de robustez (`resultados/robustez`) confirmou os quatro primeiros casos. Nele, P1 foi
morto com `kill` enquanto dormia 30 s na RC e P2, que estava na fila, recebeu o `GRANT` no mesmo
milissegundo.

## 7. Processo cliente

O cliente usa **um único socket** conectado ao IP e à porta do coordenador (argumentos `--host` e
`--porta`). Em cada uma das *r* iterações ele envia `REQUEST` e bloqueia até receber um `GRANT` com o
próprio pid (qualquer outra resposta encerra o processo com erro). Depois abre `resultado.txt` em modo
append, grava `"<pid> <AAAA-MM-DD HH:MM:SS.mmm>"`, fecha o arquivo, executa `time.sleep(k)` e envia
`RELEASE`. Abrir e fechar o arquivo a cada acesso garante que o conteúdo vá para o disco **dentro** da
RC, antes que o próximo processo escreva.

## 8. Execução, medição e verificação

`executar.py` sobe o coordenador, espera ele imprimir `PRONTO` e dispara os *n* processos **em
sequência e sem retardo** (`subprocess.Popen` em laço). Ao final envia os comandos `1`, `2` e `3` para a
interface do coordenador pelo `stdin`, o que também exercita a interface, e chama `verificar.py`. Este
último checa:

* **`resultado.txt`**: tem exatamente *n·r* linhas, os horários estão em ordem não decrescente
  (acompanham o relógio do sistema) e cada pid 1..*n* aparece exatamente *r* vezes;
* **log**: há *n·r* mensagens de cada tipo; `GRANT` e `RELEASE` se **intercalam** e cada `RELEASE`
  vem do processo que recebeu o `GRANT` anterior; a ordem dos processos nos `REQUEST` é igual à ordem
  nos `GRANT` (FIFO) e nos `RELEASE`; os instantes do log não decrescem.

## 9. Estudos de caso

Ambiente: Windows, Python 3.12, todos os processos na mesma máquina e escrevendo no mesmo
`resultado.txt`. Comando: `python src/executar.py --estudos`. O tempo total vai do disparo do primeiro
processo até o término do último.

| n | r | k (s) | linhas (n·r) | tempo total (s) | n·r·k (s) | tempo/acesso (ms) | verificação |
|---|---|---|---|---|---|---|---|
| 2 | 10 | 0,20 | 20 | 4,12 | 4,0 | 206,2 | todas OK |
| 4 | 10 | 0,10 | 40 | 4,12 | 4,0 | 103,0 | todas OK |
| 8 | 10 | 0,05 | 80 | 4,17 | 4,0 | 52,1 | todas OK |
| 16 | 10 | 0 | 160 | 0,19 | 0 | 1,2 | todas OK |
| 32 | 20 | 0 | 640 | 0,44 | 0 | 0,7 | todas OK |

**Análise.**

* **Corretude.** Todos os cenários passaram em todas as verificações: número de linhas, r escritas por
  processo, horários monotônicos, intercalação GRANT/RELEASE e ordem REQUEST = GRANT = RELEASE. Isso
  vale até para 32 processos competindo sem pausa (k = 0), o caso de maior concorrência.
* **Serialização.** Nos três primeiros cenários *n·r·k* = 4 s e o tempo medido ficou entre 4,12 e
  4,17 s. A RC é de fato executada em série: se houvesse sobreposição, o tempo ficaria abaixo de 4 s. A
  diferença (≈ 0,12–0,17 s) é o custo de criar os processos Python, conectar e trocar as mensagens.
* **Custo do protocolo.** Com k = 0 o tempo por acesso fica em ≈ 1 ms, já somando 3 mensagens TCP
  locais, abertura e fechamento do arquivo e a troca de contexto entre threads. Esse é o limite de
  vazão do coordenador: ele atende uma RC por vez, então a vazão máxima é ≈ 1 / (k + custo do
  protocolo).
* **Justiça.** Com a fila FIFO, os processos se alternam em rodízio: `1,2,3,4,1,2,3,4,…` em
  `resultado.txt` com n = 4. Nenhum processo é atendido duas vezes enquanto outro espera.
* **Limitações do algoritmo centralizado**, como esperado: o coordenador é **ponto único de falha** e
  **gargalo** (cada acesso exige 3 mensagens passando por ele). Em troca, o algoritmo é simples, justo e
  usa poucas mensagens por acesso.

## 10. Como executar

```bash
cd src
python executar.py -n 4 -r 10 -k 0.1        # um cenário + verificação (saída em resultados/)
python executar.py --estudos                # todos os estudos de caso da seção 9
# Execução manual (coordenador interativo em um terminal, processos em outros):
python coordenador.py --porta 5000          # comandos: 1 = fila, 2 = atendimentos, 3 = encerrar
python processo.py --id 1 -r 5 -k 1 --porta 5000
python verificar.py -n 4 -r 10 --dir ../resultados/n4_r10_k0.1
```

## 11. Conclusão

O algoritmo centralizado foi implementado com o monitor `bounded_buffer` exigido, usado na fila FIFO de
pedidos e no canal de liberações. Com isso a exclusão mútua, a ordem de chegada e a alternância
GRANT/RELEASE são garantidas pela própria estrutura do código. As verificações automáticas confirmaram
a corretude em todos os cenários (até 32 processos e 640 acessos). Os tempos medidos mostram a
serialização da RC e o baixo custo do protocolo. A implementação também tolera falhas de clientes e
mensagens inválidas.
