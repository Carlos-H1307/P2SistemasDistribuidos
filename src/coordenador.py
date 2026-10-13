"""Coordenador do algoritmo centralizado de exclusão mútua distribuída.

Threads:
  - aceitar:     aceita conexões TCP e cria uma thread receptora por processo;
  - receptoras:  leem REQUEST/RELEASE de cada socket e alimentam os monitores;
  - algoritmo:   retira pedidos da fila (FIFO), envia GRANT e aguarda o RELEASE;
  - interface:   atende comandos do terminal (fila, atendimentos, encerrar).
"""
from __future__ import annotations

import argparse
import logging
import socket
import sys
import threading

from mensagens import Mensagem, Tipo, enviar, receber
from monitor import BoundedBuffer


def criar_log(caminho: str) -> logging.Logger:
    log = logging.getLogger("coordenador")
    log.setLevel(logging.INFO)
    handler = logging.FileHandler(caminho, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d | %(message)s", "%Y-%m-%d %H:%M:%S"))
    log.addHandler(handler)
    return log


class Coordenador:
    def __init__(self, host: str, porta: int, capacidade: int, log: logging.Logger) -> None:
        self._servidor = socket.create_server((host, porta), backlog=128)
        self._log = log
        # Fila de pedidos de acesso à região crítica (monitor do pseudocódigo)
        self._pedidos: BoundedBuffer[int] = BoundedBuffer(capacidade)
        # Canal de liberações: o algoritmo bloqueia aqui até o detentor devolver a região crítica
        self._liberacoes: BoundedBuffer[int] = BoundedBuffer(capacidade)
        # Protege conexões, atendimentos e detentor (compartilhados entre todas as threads)
        self._estado = threading.Lock()
        self._conexoes: dict[int, socket.socket] = {}
        self._atendimentos: dict[int, int] = {}
        self._detentor: int | None = None
        # Garante que a ordem dos REQUEST no log seja a mesma da inserção na fila
        self._ordem_pedidos = threading.Lock()

    # ---------------------------------------------------------------- log
    def _registrar(self, direcao: str, mensagem: Mensagem) -> None:
        self._log.info("%s | %s | P%d", direcao, mensagem.tipo.name, mensagem.pid)

    # ---------------------------------------------------------------- thread de conexões
    def aceitar(self) -> None:
        while True:
            try:
                conexao, endereco = self._servidor.accept()
            except OSError:
                return  # socket servidor fechado no encerramento
            threading.Thread(target=self._receber, args=(conexao, endereco), daemon=True).start()

    # ---------------------------------------------------------------- threads receptoras
    def _receber(self, conexao: socket.socket, endereco: tuple[str, int]) -> None:
        pid: int | None = None
        try:
            while (mensagem := receber(conexao)) is not None:
                if pid is None:
                    if not self._associar(mensagem.pid, conexao):
                        self._log.info("INFO | pid %d já conectado, conexão %s recusada", mensagem.pid, endereco)
                        return
                    pid = mensagem.pid
                if mensagem.pid != pid:
                    self._log.info("INFO | mensagem com pid %d ignorada na conexão de P%d", mensagem.pid, pid)
                elif mensagem.tipo is Tipo.REQUEST:
                    with self._ordem_pedidos:
                        self._registrar("RECV", mensagem)
                        self._pedidos.put_resource(pid)
                elif mensagem.tipo is Tipo.RELEASE:
                    self._liberar(pid, mensagem)
                else:
                    self._log.info("INFO | P%d enviou %s, ignorada", pid, mensagem.tipo.name)
        except (ValueError, OSError) as erro:
            self._log.info("INFO | conexão %s encerrada com erro: %s", endereco, erro)
        finally:
            conexao.close()
            if pid is not None:
                self._desassociar(pid)

    def _associar(self, pid: int, conexao: socket.socket) -> bool:
        with self._estado:
            if pid in self._conexoes:
                return False
            self._conexoes[pid] = conexao
            self._atendimentos.setdefault(pid, 0)
            return True

    def _liberar(self, pid: int, mensagem: Mensagem) -> None:
        with self._estado:
            valido = self._detentor == pid
            if valido:
                self._detentor = None
        if not valido:
            self._log.info("INFO | RELEASE de P%d sem possuir a região crítica, ignorado", pid)
            return
        self._registrar("RECV", mensagem)
        self._liberacoes.put_resource(pid)

    def _desassociar(self, pid: int) -> None:
        with self._estado:
            self._conexoes.pop(pid, None)
            caiu_na_rc = self._detentor == pid
            if caiu_na_rc:
                self._detentor = None
        if caiu_na_rc:
            # Processo morreu dentro da região crítica: libera para não travar os demais
            self._log.info("INFO | P%d desconectou na região crítica, liberação forçada", pid)
            self._liberacoes.put_resource(pid)

    # ---------------------------------------------------------------- thread do algoritmo
    def algoritmo(self) -> None:
        while True:
            pid = self._pedidos.get_resource()
            with self._estado:
                conexao = self._conexoes.get(pid)
                if conexao is None:
                    continue  # processo desconectou enquanto esperava na fila
                self._detentor = pid
                self._atendimentos[pid] += 1
            grant = Mensagem(Tipo.GRANT, pid)
            # Registrado antes do envio para que o RELEASE nunca apareça antes do GRANT no log
            self._registrar("SEND", grant)
            try:
                enviar(conexao, grant)
            except OSError:
                pass  # a thread receptora detecta a queda e força a liberação
            self._liberacoes.get_resource()

    # ---------------------------------------------------------------- thread de interface
    def interface(self) -> None:
        print("Comandos: 1 = fila de pedidos | 2 = atendimentos por processo | 3 = encerrar")
        for linha in sys.stdin:
            comando = linha.strip()
            if comando == "1":
                with self._estado:
                    detentor = self._detentor
                fila = self._pedidos.snapshot()
                print(f"Na região crítica: {'P' + str(detentor) if detentor is not None else '-'}")
                print(f"Fila ({len(fila)}): {' -> '.join(f'P{p}' for p in fila) or 'vazia'}")
            elif comando == "2":
                with self._estado:
                    atendimentos = sorted(self._atendimentos.items())
                for pid, total in atendimentos:
                    print(f"P{pid}: {total}")
                if not atendimentos:
                    print("Nenhum processo conectado até agora")
            elif comando == "3":
                return
            elif comando:
                print("Comando inválido")

    def encerrar(self) -> None:
        self._servidor.close()
        with self._estado:
            conexoes = list(self._conexoes.values())
        for conexao in conexoes:
            conexao.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Coordenador de exclusão mútua centralizada")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--capacidade", type=int, default=256, help="N do bounded buffer")
    parser.add_argument("--log", default="coordenador.log")
    args = parser.parse_args()

    log = criar_log(args.log)
    coordenador = Coordenador(args.host, args.porta, args.capacidade, log)
    threading.Thread(target=coordenador.aceitar, name="aceitar", daemon=True).start()
    threading.Thread(target=coordenador.algoritmo, name="algoritmo", daemon=True).start()
    interface = threading.Thread(target=coordenador.interface, name="interface")
    interface.start()
    print(f"PRONTO em {args.host}:{args.porta}", flush=True)
    interface.join()
    coordenador.encerrar()
    logging.shutdown()


if __name__ == "__main__":
    main()
