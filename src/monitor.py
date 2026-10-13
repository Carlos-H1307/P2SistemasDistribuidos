"""Monitor bounded_buffer, implementado conforme o pseudocódigo da disciplina.

    Monitor bounded_buffer {
      Resource buffer[N];
      // Variables for indexing buffer
      Condition not_full, not_empty;
      void put_resource(Resource R) { while (full) wait(not_full); add R; signal(not_empty); }
      Resource get_resource()       { while (empty) wait(not_empty); get R; signal(not_full); return R; }
    }

A exclusão mútua implícita do monitor é o Lock compartilhado pelas duas variáveis de
condição. As condições do Python seguem a semântica de Mesa (quem acorda não recebe o
monitor imediatamente), por isso o `while` do pseudocódigo é obrigatório e não um `if`.
"""
from __future__ import annotations

from threading import Condition, Lock
from typing import Generic, TypeVar

T = TypeVar("T")


class BoundedBuffer(Generic[T]):
    def __init__(self, n: int) -> None:
        if n < 1:
            raise ValueError("N deve ser >= 1")
        self._buffer: list[T | None] = [None] * n
        self._n = n
        # Variáveis de indexação do buffer circular
        self._entrada = 0
        self._saida = 0
        self._quantidade = 0
        self._monitor = Lock()
        self._not_full = Condition(self._monitor)
        self._not_empty = Condition(self._monitor)

    def put_resource(self, recurso: T) -> None:
        with self._monitor:
            while self._quantidade == self._n:
                self._not_full.wait()
            self._buffer[self._entrada] = recurso
            self._entrada = (self._entrada + 1) % self._n
            self._quantidade += 1
            self._not_empty.notify()

    def get_resource(self) -> T:
        with self._monitor:
            while self._quantidade == 0:
                self._not_empty.wait()
            recurso = self._buffer[self._saida]
            self._buffer[self._saida] = None
            self._saida = (self._saida + 1) % self._n
            self._quantidade -= 1
            self._not_full.notify()
        if recurso is None:
            raise RuntimeError("posição vazia lida do buffer")
        return recurso

    def snapshot(self) -> list[T]:
        """Cópia do conteúdo em ordem FIFO, lida dentro do monitor (usada pela interface)."""
        with self._monitor:
            indices = ((self._saida + i) % self._n for i in range(self._quantidade))
            return [r for r in (self._buffer[i] for i in indices) if r is not None]
