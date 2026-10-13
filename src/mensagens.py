"""Protocolo de mensagens de tamanho fixo trocadas entre coordenador e processos.

Formato: <tipo>|<pid>|<preenchimento com zeros>, sempre com F bytes ASCII.
Exemplo (F = 10): REQUEST do processo 3 -> "1|3|000000".
"""
from __future__ import annotations

import socket
from dataclasses import dataclass
from enum import IntEnum

F = 10
SEPARADOR = "|"
PREENCHIMENTO = "0"
PID_MAXIMO = 10 ** (F - 4) - 1  # "t|" + "|" ocupam 3 bytes, sobra F-3 para pid + ao menos 1 de preenchimento


class Tipo(IntEnum):
    REQUEST = 1
    GRANT = 2
    RELEASE = 3


@dataclass(frozen=True)
class Mensagem:
    tipo: Tipo
    pid: int

    def codificar(self) -> bytes:
        if not 0 <= self.pid <= PID_MAXIMO:
            raise ValueError(f"pid fora do intervalo 0..{PID_MAXIMO}: {self.pid}")
        corpo = f"{int(self.tipo)}{SEPARADOR}{self.pid}{SEPARADOR}"
        return corpo.ljust(F, PREENCHIMENTO).encode("ascii")

    @staticmethod
    def decodificar(dados: bytes) -> Mensagem:
        if len(dados) != F:
            raise ValueError(f"mensagem com {len(dados)} bytes (esperado {F})")
        try:
            texto = dados.decode("ascii")
        except UnicodeDecodeError as erro:
            raise ValueError("mensagem não é ASCII") from erro
        campos = texto.split(SEPARADOR)
        if (
            len(campos) != 3
            or not campos[0].isdigit()
            or not campos[1].isdigit()
            or campos[2].strip(PREENCHIMENTO)
        ):
            raise ValueError(f"mensagem mal formada: {texto!r}")
        return Mensagem(Tipo(int(campos[0])), int(campos[1]))


def enviar(sock: socket.socket, mensagem: Mensagem) -> None:
    sock.sendall(mensagem.codificar())


def receber(sock: socket.socket) -> Mensagem | None:
    """Lê exatamente F bytes (TCP é um fluxo, recv pode devolver pedaços). None = conexão encerrada."""
    buffer = bytearray()
    while len(buffer) < F:
        parte = sock.recv(F - len(buffer))
        if not parte:
            if buffer:
                raise ConnectionError("conexão encerrada no meio de uma mensagem")
            return None
        buffer += parte
    return Mensagem.decodificar(bytes(buffer))
