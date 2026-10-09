"""Evidências do pipeline: pente fino, exportações em CSV e gráficos dos insights.

Tudo é gerado a partir das tabelas publicadas (nada calculado à parte), para que as evidências
do repositório sejam exatamente o que o job produziu.

`tabela(nome)` resolve o nome lógico ("ms_gold.market_share") para o nome físico — no Databricks
"<catalog>.ms_gold.market_share"; nos testes, uma view temporária.
"""
from __future__ import annotations

import os
import re
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

Resolver = Callable[[str], str]

# Paleta de referência (validada para daltonismo), superfície clara
SUPERFICIE, TINTA, TINTA2, GRADE = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]

CATEGORIAS = {  # rótulo de exibição (a origem vem em maiúsculas sem acento)
    "BEBIDAS": "Bebidas", "CHOCOLATES": "Chocolates", "CULINARIOS": "Culinários",
    "LACTEOS": "Lácteos", "NUTRICAO": "Nutrição",
}

NOMES_REGRAS = {
    "FCT_001": "FCT_001 duplicata exata", "FCT_002": "FCT_002 versões conflitantes",
    "FCT_003": "FCT_003 mesma venda A e B", "FCT_004": "FCT_004 loja inexistente",
    "FCT_005": "FCT_005 EAN inexistente", "FCT_008": "FCT_008 negativo sem prova",
    "FCT_009": "FCT_009 preço fora da faixa", "FCT_010": "FCT_010 pico de carga",
}


def _brl(v: float) -> str:
    """Formato brasileiro: 1.234,5"""
    return f"{v:,.1f}".replace(",", ";").replace(".", ",").replace(";", ".")


def _milhar(n: float) -> str:
    """Inteiro com separador de milhar brasileiro: 2.191"""
    return f"{n:,.0f}".replace(",", ".")


# ------------------------------------------------------------------ pente fino
def pente_fino(spark: SparkSession, sql_texto: str, catalog: str) -> DataFrame:
    """Executa sql/validacao/00_pente_fino.sql trocando o catálogo fixo pelo do ambiente."""
    sql = re.sub(r"--[^\n]*", "", sql_texto).strip().rstrip(";")
    return spark.sql(sql.replace("workspace.", f"{catalog}."))


# ------------------------------------------------------------------ exportações
def ultimo_run(spark: SparkSession, tabela: Resolver) -> str:
    return (spark.table(tabela("ms_dq.rule_results")).where("alvo = 'cobertura'")
                 .agg(F.max_by("run_id", "executado_em")).first()[0])


def exportar_csvs(spark: SparkSession, tabela: Resolver, destino: str) -> dict[str, int]:
    """Grava os CSVs de evidência em `destino` (pasta local ou Volume) e devolve as linhas de cada um."""
    os.makedirs(destino, exist_ok=True)
    run = ultimo_run(spark, tabela)
    q = spark.table(tabela("ms_dq.quarantine"))
    saidas = {
        "rule_results": (spark.table(tabela("ms_dq.rule_results")).where(F.col("run_id") == run)
                         .drop("criterio", "tratamento").orderBy("alvo", "regra_id")),
        "quarentena_resumo": (q.groupBy("alvo", "regra_id", "classificacao", "severidade")
                              .agg(F.count("*").alias("linhas"),
                                   F.round(F.sum(F.abs("valor_brl")), 2).alias("valor_retido_brl"),
                                   F.min("motivo").alias("exemplo_motivo"))
                              .orderBy("alvo", "regra_id")),
        "quarentena_amostra": (q.where("alvo = 'fato_vendas'")
                               .withColumn("_n", F.row_number().over(Window.partitionBy("regra_id").orderBy("chave")))
                               .where("_n <= 5")
                               .select("regra_id", "classificacao", "arquivo_origem", "chave", "motivo", "valor_brl")
                               .orderBy("regra_id", "chave")),
        "market_share_nacional": (spark.table(tabela("ms_gold.market_share")).where("level = 'NACIONAL'")
                                  .orderBy("year_week", "category", "ms_rank")),
        "fato_vendas_amostra": (spark.table(tabela("ms_gold.fato_vendas"))
                                .where(F.col("year_week") == F.lit(_ultima_semana_oficial(spark, tabela)))
                                .drop("dq_flags").orderBy("store_id", "ean").limit(500)),
    }
    linhas = {}
    for nome, df in saidas.items():
        pdf = df.toPandas()                      # todos pequenos (agregados ou amostras)
        pdf.to_csv(os.path.join(destino, f"{nome}.csv"), index=False)
        linhas[nome] = len(pdf)
    return linhas


def _ultima_semana_oficial(spark: SparkSession, tabela: Resolver) -> str:
    return (spark.table(tabela("ms_gold.market_share"))
                 .where("level = 'NACIONAL' AND ms_status = 'OFICIAL'")
                 .agg(F.max("year_week")).first()[0])


# ------------------------------------------------------------------ gráficos
def _estilo(plt):
    plt.rcParams.update({
        "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "axes.edgecolor": GRADE,
        "axes.labelcolor": TINTA2, "xtick.color": TINTA2, "ytick.color": TINTA2, "text.color": TINTA,
        "font.size": 10, "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
        "grid.color": GRADE, "grid.linewidth": 0.8, "axes.axisbelow": True,
    })


def gerar_graficos(spark: SparkSession, tabela: Resolver, destino: str) -> list[str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    _estilo(plt)
    os.makedirs(destino, exist_ok=True)
    ms = spark.table(tabela("ms_gold.market_share"))
    arquivos = []

    arquivos.append(_grafico_share_nacional(plt, ms, destino))

    # 2. valor retido por regra da fato
    r = (spark.table(tabela("ms_dq.quarantine")).where("alvo = 'fato_vendas'").groupBy("regra_id")
              .agg(F.count("*").alias("linhas"), F.sum(F.abs("valor_brl")).alias("v")).toPandas().sort_values("v"))
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.barh([NOMES_REGRAS.get(x, x) for x in r.regra_id], r.v / 1e3, color=SERIES[0], height=0.6)
    for i, (v, l) in enumerate(zip(r.v / 1e3, r.linhas)):
        ax.text(v + r.v.max() / 1e3 * 0.012, i, f"R$ {_milhar(v)} mil · {_milhar(l)} linhas",
                va="center", color=TINTA2, fontsize=9)
    ax.set_xlabel("Valor retido (R$ mil)")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, r.v.max() / 1e3 * 1.45)
    # cada número formatado isoladamente: um replace no título inteiro desfazia a vírgula decimal do _brl
    ax.set_title(f"Fora do Market Share: R$ {_brl(r.v.sum() / 1e6)} mi em {_milhar(r.linhas.sum())} linhas",
                 loc="left", fontsize=12)
    arquivos.append(_salvar(fig, plt, destino, "02_valor_retido_por_regra.png"))

    # 3. % de células oficiais por recorte (barra 100% empilhada)
    ordem_n = ["NACIONAL", "CANAL", "UF", "REDE", "TERRITORIO"]
    ordem_s = ["OFICIAL", "NAO_OFICIAL_COBERTURA", "NAO_OFICIAL_QUARENTENA", "NAO_OFICIAL_SEMANA_ABERTA"]
    rotulo = {"OFICIAL": "Oficial", "NAO_OFICIAL_COBERTURA": "Cobertura < 80%",
              "NAO_OFICIAL_QUARENTENA": "Quarentena > 5%", "NAO_OFICIAL_SEMANA_ABERTA": "Semana aberta"}
    st = ms.groupBy("level", "ms_status").count().toPandas()
    p = st.pivot(index="level", columns="ms_status", values="count").fillna(0).reindex(ordem_n)
    p = p.div(p.sum(axis=1), axis=0) * 100
    fig, ax = plt.subplots(figsize=(9, 3.8))
    esquerda = [0.0] * len(p)
    for i, s in enumerate(ordem_s):
        v = p[s] if s in p else [0.0] * len(p)
        ax.barh(ordem_n, v, left=esquerda, color=SERIES[i], height=0.6, label=rotulo[s],
                edgecolor=SUPERFICIE, linewidth=2)
        if s == "OFICIAL":
            for j, val in enumerate(v):
                ax.text(val - 1.5, j, f"{val:.0f}%", ha="right", va="center", color="white",
                        fontsize=9, fontweight="bold")
        esquerda = [a + b for a, b in zip(esquerda, v)]
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("% das células (semana × categoria × marca)")
    ax.grid(axis="y", visible=False)
    ax.legend(ncol=4, loc="upper left", bbox_to_anchor=(0, -0.18), frameon=False, fontsize=9)
    ax.set_title("Quanto do Market Share é oficial, por recorte", loc="left", fontsize=12)
    arquivos.append(_salvar(fig, plt, destino, "03_status_por_nivel.png"))

    # 4. cobertura semanal por fornecedor, com o portão de 80%
    c = (spark.table(tabela("ms_silver.cobertura_fornecedor")).groupBy("provider", "year_week")
              .agg(F.avg("coverage_ratio").alias("c")).toPandas().sort_values("year_week"))
    fig, ax = plt.subplots(figsize=(10, 3.6))
    for i, pv in enumerate(sorted(c.provider.unique())):
        d = c[c.provider == pv]
        ax.plot(range(len(d)), d.c * 100, color=SERIES[i], lw=2, label=f"Fornecedor {pv}")
        ax.text(len(d) - 0.5, d.c.iloc[-1] * 100, pv, color=TINTA2, va="center", fontsize=9)
    semanas = sorted(c.year_week.unique())
    ax.axhline(80, color=TINTA2, lw=1, ls="--")
    ax.text(len(semanas) / 2 - 0.5, 60.8, "linha tracejada = portão de 80% (abaixo: MS não oficial)",
            color=TINTA2, fontsize=9, ha="right")
    ax.set_xticks(range(0, len(semanas), 13))
    ax.set_xticklabels(semanas[::13])
    ax.set_ylabel("Cobertura média das redes (%)")
    ax.set_ylim(60, 100)
    ax.legend(frameon=False, loc="lower left", ncol=2)
    ax.set_title("Cobertura declarada pelos fornecedores, por semana", loc="left", fontsize=12)
    arquivos.append(_salvar(fig, plt, destino, "04_cobertura_semanal.png"))
    return arquivos


def _grafico_share_nacional(plt, ms: DataFrame, destino: str) -> str:
    """Share Nestlé nacional por categoria (um painel por categoria).

    A série semanal oscila ~6 p.p. de uma semana para outra sem persistência (autocorrelação ≈ 0),
    então a leitura de negócio é a média móvel de 4 semanas, calculada só com semanas oficiais.
    A semanal fica ao fundo, com as semanas não oficiais marcadas.
    """
    # semana não oficial se QUALQUER marca da célula for não oficial (explícito, não depende de ordem alfabética)
    n = (ms.where("level = 'NACIONAL' AND is_own_product = 'Y'").groupBy("year_week", "category")
           .agg(F.sum("ms_value").alias("s"),
                F.max(F.when(F.col("ms_status") != "OFICIAL", 1).otherwise(0)).alias("nao_oficial"))
           .toPandas().sort_values("year_week"))
    cats = sorted(n.category.unique())
    fig, axs = plt.subplots(1, len(cats), figsize=(15, 3.6), sharey=True)
    for ax, c in zip(axs, cats):
        d = n[n.category == c].reset_index(drop=True)
        oficial = d.s.where(d.nao_oficial == 0) * 100
        mm4 = oficial.rolling(4, min_periods=3).mean()
        ax.plot(d.index, d.s * 100, color=SERIES[0], lw=1, alpha=0.35)
        ax.plot(d.index, mm4, color=SERIES[0], lw=2.2)
        nao = d[d.nao_oficial == 1]
        ax.scatter(nao.index, nao.s * 100, s=30, facecolor=SUPERFICIE, edgecolor=SERIES[0], lw=1.5, zorder=3)
        ax.set_title(f"{CATEGORIAS.get(c, c.title())} · média oficial {oficial.mean():.0f}%",
                     fontsize=11, color=TINTA, loc="left")
        ax.set_ylim(0, 65)
        ax.set_xticks([0, len(d) - 1])
        ax.set_xticklabels([d.year_week.iloc[0], d.year_week.iloc[-1]])
    axs[0].set_ylabel("Share Nestlé (%, valor)")
    fig.suptitle("Share Nestlé nacional por categoria · linha forte = média móvel de 4 semanas oficiais; "
                 "fundo = semanal; ○ = semana não oficial", x=0.01, ha="left", fontsize=12)
    return _salvar(fig, plt, destino, "01_share_nestle_nacional.png")


def _salvar(fig, plt, destino: str, nome: str) -> str:
    caminho = os.path.join(destino, nome)
    fig.tight_layout()
    fig.savefig(caminho, dpi=150)
    plt.close(fig)
    return caminho