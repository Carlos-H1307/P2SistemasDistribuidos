"""Verifica a corretude de uma execução a partir de resultado.txt e do log do coordenador."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
from pathlib import Path

FORMATO_HORA = "%Y-%m-%d %H:%M:%S.%f"


def verificar_resultado(caminho: Path, n: int, r: int) -> list[tuple[str, bool]]:
    checks: list[tuple[str, bool]] = []
    linhas = caminho.read_text(encoding="utf-8").splitlines()
    checks.append((f"resultado.txt tem n*r = {n * r} linhas (tem {len(linhas)})", len(linhas) == n * r))

    pids: list[int] = []
    horas: list[datetime] = []
    for linha in linhas:
        pid, _, hora = linha.partition(" ")
        try:
            pids.append(int(pid))
            horas.append(datetime.strptime(hora, FORMATO_HORA))
        except ValueError:
            checks.append((f"linha mal formada: {linha!r}", False))
            return checks
    checks.append(("horários em ordem não decrescente", all(a <= b for a, b in zip(horas, horas[1:]))))
    contagem = Counter(pids)
    checks.append((f"cada processo 1..{n} escreveu r = {r} vezes",
            contagem == Counter({p: r for p in range(1, n + 1)})))
    return checks


def verificar_log(caminho: Path, n: int, r: int) -> list[tuple[str, bool]]:
    checks: list[tuple[str, bool]] = []
    eventos: list[tuple[datetime, str, int]] = []
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        campos = linha.split(" | ")
        if len(campos) == 4 and campos[1] in ("RECV", "SEND"):
            eventos.append((datetime.strptime(campos[0], FORMATO_HORA), campos[2], int(campos[3][1:])))

    por_tipo = {t: [pid for _, tipo, pid in eventos if tipo == t] for t in ("REQUEST", "GRANT", "RELEASE")}
    for tipo, pids in por_tipo.items():
        checks.append((f"{tipo}: {len(pids)} mensagens (esperado {n * r})", len(pids) == n * r))

    sequencia = [(tipo, pid) for _, tipo, pid in eventos if tipo != "REQUEST"]
    intercalado = all(
        tipo == ("GRANT" if i % 2 == 0 else "RELEASE") and (i % 2 == 0 or pid == sequencia[i - 1][1])
        for i, (tipo, pid) in enumerate(sequencia)
    )
    checks.append(("GRANT e RELEASE intercalados, cada RELEASE vindo do processo que recebeu o GRANT", intercalado))
    checks.append(("ordem dos processos em REQUEST == ordem em GRANT (FIFO)", por_tipo["REQUEST"] == por_tipo["GRANT"]))
    checks.append(("ordem dos processos em REQUEST == ordem em RELEASE", por_tipo["REQUEST"] == por_tipo["RELEASE"]))
    checks.append(("instantes do log em ordem não decrescente",
            all(a[0] <= b[0] for a, b in zip(eventos, eventos[1:]))))
    return checks


def verificar(diretorio: Path, n: int, r: int, log: str = "coordenador.log") -> bool:
    checks = verificar_resultado(diretorio / "resultado.txt", n, r) + verificar_log(diretorio / log, n, r)
    for descricao, ok in checks:
        print(f"  [{'OK' if ok else 'FALHA'}] {descricao}")
    return all(ok for _, ok in checks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verifica resultado.txt e o log do coordenador")
    parser.add_argument("-n", type=int, required=True)
    parser.add_argument("-r", type=int, required=True)
    parser.add_argument("--dir", type=Path, default=Path("."))
    args = parser.parse_args()
    raise SystemExit(0 if verificar(args.dir, args.n, args.r) else 1)


if __name__ == "__main__":
    main()
