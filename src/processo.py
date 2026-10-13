"""Processo cliente: pede a região crítica r vezes, registrando pid e hora em resultado.txt."""
from __future__ import annotations

import argparse
import socket
import time
from datetime import datetime

from mensagens import PID_MAXIMO, Mensagem, Tipo, enviar, receber


def regiao_critica(pid: int, arquivo: str, k: float) -> None:
    with open(arquivo, "a", encoding="utf-8") as saida:
        saida.write(f"{pid} {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n")
    time.sleep(k)


def executar(host: str, porta: int, pid: int, r: int, k: float, arquivo: str) -> None:
    with socket.create_connection((host, porta)) as sock:
        for _ in range(r):
            enviar(sock, Mensagem(Tipo.REQUEST, pid))
            resposta = receber(sock)
            if resposta != Mensagem(Tipo.GRANT, pid):
                raise SystemExit(f"P{pid}: resposta inesperada do coordenador: {resposta}")
            regiao_critica(pid, arquivo, k)
            enviar(sock, Mensagem(Tipo.RELEASE, pid))


def main() -> None:
    parser = argparse.ArgumentParser(description="Processo que disputa a região crítica")
    parser.add_argument("--id", type=int, required=True, dest="pid")
    parser.add_argument("-r", type=int, required=True, help="repetições")
    parser.add_argument("-k", type=float, required=True, help="segundos dentro da região crítica")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--arquivo", default="resultado.txt")
    args = parser.parse_args()
    if not 0 <= args.pid <= PID_MAXIMO or args.r < 0 or args.k < 0:
        parser.error(f"exige 0 <= id <= {PID_MAXIMO}, r >= 0 e k >= 0")
    executar(args.host, args.porta, args.pid, args.r, args.k, args.arquivo)


if __name__ == "__main__":
    main()
