"""Executa um cenário (coordenador + n processos iniciados em sequência, sem retardo) e verifica.

Uso:
  python executar.py -n 4 -r 10 -k 0.1     # um cenário
  python executar.py --estudos             # todos os estudos de caso do relatório
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from verificar import verificar

SRC = Path(__file__).resolve().parent
ESTUDOS: list[tuple[int, int, float]] = [(2, 10, 0.2), (4, 10, 0.1), (8, 10, 0.05), (16, 10, 0.0), (32, 20, 0.0)]
AMBIENTE = {**os.environ, "PYTHONIOENCODING": "utf-8"}


@dataclass(frozen=True)
class Resultado:
    n: int
    r: int
    k: float
    duracao: float
    correto: bool


def executar_cenario(n: int, r: int, k: float, porta: int, diretorio: Path) -> Resultado:
    diretorio.mkdir(parents=True, exist_ok=True)
    (diretorio / "resultado.txt").unlink(missing_ok=True)

    coordenador = subprocess.Popen(
        [sys.executable, "-u", str(SRC / "coordenador.py"), "--porta", str(porta)],
        cwd=diretorio, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8", env=AMBIENTE,
    )
    if coordenador.stdout is None or coordenador.stdin is None:
        raise RuntimeError("pipes do coordenador indisponíveis")
    if not any("PRONTO" in linha for linha in coordenador.stdout):
        raise RuntimeError("coordenador não iniciou")

    inicio = time.perf_counter()
    processos = [
        subprocess.Popen(
            [sys.executable, str(SRC / "processo.py"), "--id", str(pid), "-r", str(r), "-k", str(k), "--porta", str(porta)],
            cwd=diretorio,
        )
        for pid in range(1, n + 1)
    ]
    falhas = sum(p.wait() != 0 for p in processos)
    duracao = time.perf_counter() - inicio

    saida, _ = coordenador.communicate("1\n2\n3\n", timeout=30)
    print(f"\n=== n={n} r={r} k={k} | {duracao:.2f} s ===")
    print(saida.strip())
    correto = verificar(diretorio, n, r) and falhas == 0
    return Resultado(n, r, k, duracao, correto)


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa e avalia cenários de exclusão mútua")
    parser.add_argument("-n", type=int, default=4)
    parser.add_argument("-r", type=int, default=10)
    parser.add_argument("-k", type=float, default=0.1)
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--estudos", action="store_true", help="executa todos os estudos de caso")
    args = parser.parse_args()

    cenarios = ESTUDOS if args.estudos else [(args.n, args.r, args.k)]
    resultados = [
        executar_cenario(n, r, k, args.porta + i, SRC.parent / "resultados" / f"n{n}_r{r}_k{k}")
        for i, (n, r, k) in enumerate(cenarios)
    ]

    print("\n| n | r | k (s) | linhas | tempo total (s) | tempo/acesso (ms) | correto |")
    print("|---|---|---|---|---|---|---|")
    for res in resultados:
        acessos = res.n * res.r
        print(f"| {res.n} | {res.r} | {res.k} | {acessos} | {res.duracao:.2f} | "
              f"{1000 * res.duracao / acessos:.1f} | {'sim' if res.correto else 'NÃO'} |")
    raise SystemExit(0 if all(res.correto for res in resultados) else 1)


if __name__ == "__main__":
    main()
