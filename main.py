import streamlit as st
import pandas as pd
import re
from supabase import create_client

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================
st.set_page_config(page_title="Civil Gestão", page_icon="🏛️", layout="wide")

@st.cache_resource
def iniciar_conexao():
    import os
    url   = os.environ.get("SUPABASE_URL", "https://crdskgqkkkzmgedsunat.supabase.co")
    chave = os.environ.get("SUPABASE_KEY", "sb_publishable_MTT7fwe8IefxrY6MO_gthw_nCo5QfWF")
    return create_client(url, chave)

supabase = iniciar_conexao()

# ==========================================================
# NAVEGAÇÃO
# ==========================================================
st.sidebar.title("🏛️ CIVIL GESTÃO")
st.sidebar.caption("OBRAS & MEDIÇÕES")
st.sidebar.divider()

pagina_selecionada = st.sidebar.radio("Menu Principal", [
    "--- 📊 DASHBOARDS ---", "🏭 Produção", "💰 Financeiro", "👷 Folha / RH",
    "--- 🛠️ GESTÃO ---", "🏗️ Gestão de Obras", "👥 Equipe",
    "📋 Gestão à Vista", "👤 Reunião 1:1",
    "--- 🗄️ BASE DE DADOS ---", "📥 Importador de Arquivos"
])

if "---" in pagina_selecionada:
    st.sidebar.warning("👆 Selecione uma página válida.")
    st.stop()

st.sidebar.divider()
st.sidebar.caption("⚡ v4.0 — CSVs Padronizados")

# ==========================================================
# FUNÇÕES UTILITÁRIAS
# ==========================================================

def formatar_data(serie):
    """Converte data para AAAA-MM-DD. Delega para formatar_data_valor — nunca NaN."""
    return serie.apply(formatar_data_valor)

def formatar_data_valor(val):
    """Versão para uso linha a linha (retorna string ou None, nunca NaN)."""
    if val is None or str(val).strip() in ("", "nan", "NaN", "NaT"):
        return None
    try:
        resultado = pd.to_datetime(val, dayfirst=True, errors="coerce")
        if pd.isna(resultado):
            return None
        return resultado.strftime("%Y-%m-%d")
    except Exception:
        return None

def formatar_numero(serie):
    """Remove R$, espaços, pontos de milhar, troca vírgula por ponto.
    Retorna None para vazios — nunca float NaN."""
    import math as _math
    def _conv(val):
        if pd.isna(val) or str(val).strip() in ("", "nan", "NaN"):
            return None
        try:
            limpo = (str(val)
                     .replace("R$", "")
                     .replace(" ", "")
                     .replace(".", "")
                     .replace(",", ".")
                     .strip())
            r = float(limpo)
            return None if _math.isnan(r) else r
        except Exception:
            return None
    return serie.apply(_conv)

def normalize(name):
    if pd.isna(name) or str(name).strip() == "": return ""
    return re.sub(r"\s+", " ", str(name).strip().lower())

def clean_str(val):
    if pd.isna(val) or str(val).strip() == "": return None
    return str(val).strip()

def extrair_codigo(texto):
    """Extrai os 4 primeiros dígitos numéricos. Ex: 8198-DONA → 8198"""
    if pd.isna(texto): return None
    m = re.match(r"^(\d{4})", str(texto).strip())
    return m.group(1) if m else None

def fix_ids(pacote):
    """Garante que colunas de ID sejam int puro (nunca 6.0)."""
    int_cols = ["obra_id","responsavel_id","aprovador_id","consultado_id",
                "informado_id","colaborador_id","qtde_pecas","numero_carga"]
    for row in pacote:
        for col in int_cols:
            if col in row and row[col] is not None:
                try: row[col] = int(float(row[col]))
                except: row[col] = None
    return pacote

def nulos(df):
    """Converte toda célula vazia/NaN em None puro (null no JSON).
    Sem applymap — compatível com pandas 2.x e Python 3.12."""
    import math as _m
    df = df.astype(object).where(pd.notnull(df), None)
    for col in df.columns:
        df[col] = df[col].apply(
            lambda v: None if (
                v is None
                or (isinstance(v, float) and (_m.isnan(v) or _m.isinf(v)))
                or str(v).strip().lower() in ("nan", "nat", "none", "inf", "")
            ) else v
        )
    return df

def mapa_obras():
    """Dicionário código-4-dígitos → id do banco."""
    dados = supabase.table("obras").select("id, codigo").execute().data
    m = {}
    for o in dados:
        c = str(o["codigo"]).strip()[:4] if o["codigo"] else None
        if c and c.isdigit(): m[c] = o["id"]
    return m

def aplicar_obra_id(serie_codigos, mob):
    """Mapeia códigos de obra para IDs inteiros puros.
    Retorna lista com int ou None — nunca float (evita erro bigint no Supabase)."""
    ids = []
    for cod in serie_codigos:
        v = mob.get(cod)
        ids.append(int(v) if v is not None else None)
    return pd.array(ids, dtype=object)

def mapa_equipe():
    dados = supabase.table("equipe").select("id, nome").execute().data
    return {normalize(e["nome"]): e["id"] for e in dados}

def limpar_nan_pacote(pacote):
    """Remove NaN/Inf/None residuais de um pacote pronto para insert.
    Garante que nenhum float NaN chegue ao JSON do Supabase."""
    import math as _m
    limpo = []
    for row in pacote:
        limpo.append({
            k: None if (
                v is None
                or (isinstance(v, float) and (_m.isnan(v) or _m.isinf(v)))
                or str(v).strip().lower() in ("nan", "nat", "none", "inf", "")
            ) else v
            for k, v in row.items()
        })
    return limpo

def enviar_lotes(tabela, pacote, barra_label="Enviando..."):
    """Envia em lotes de 500 com barra de progresso."""
    total = len(pacote)
    barra = st.progress(0, text=barra_label)
    enviado = 0
    for i in range(0, total, 500):
        supabase.table(tabela).insert(pacote[i:i+500]).execute()
        enviado += len(pacote[i:i+500])
        barra.progress(min(enviado/total, 1.0), text=f"{enviado}/{total} registros")
    return enviado

# ==========================================================
# IMPORTADOR
# ==========================================================
if pagina_selecionada == "📥 Importador de Arquivos":
    st.header("📥 Central de Importação")

    st.info("""
    **Ordem obrigatória:**
    FASE 1 → Equipe, Obras, Template Jornada, Rotinas
    FASE 2 → Extras, Tarefas
    FASE 3 → Fabricação, Transporte, Montagem, Custos
    FASE 4 → Notas Fiscais / Medições, Resumo Financeiro Obras
    """)

    st.warning("""
    ⚠️ **Use somente os CSVs padronizados** (arquivos `IMPORT_*.csv`).
    Eles têm os títulos exatos que o sistema espera. Copie seus dados para dentro deles.
    """)

    opcao = st.selectbox("Qual arquivo está importando?", [
        "1. [FASE 1] Equipe                  → 1_IMPORT_equipe.csv",
        "2. [FASE 1] Obras Mães              → 2_IMPORT_obras.csv",
        "3. [FASE 1] Template Jornada        → 3_IMPORT_template_jornada.csv",
        "4. [FASE 1] Rotinas                 → 4_IMPORT_rotinas.csv",
        "5. [FASE 2] Extras 1:1              → 5_IMPORT_extras.csv",
        "6. [FASE 2] Tarefas / Súmulas       → 6_IMPORT_obras_tarefas.csv",
        "7. [FASE 3] Fabricação              → 7_IMPORT_fabricacao.csv",
        "8. [FASE 3] Transporte              → 8_IMPORT_transporte.csv",
        "9. [FASE 3] Montagem                → 9_IMPORT_montagem.csv",
        "10.[FASE 3] Custos / Financeiro     → 10_IMPORT_custos.csv",
        "11.[FASE 4] Notas Fiscais / Medições  → Lista_Medição.csv",
        "12.[FASE 4] Resumo Financeiro Obras   → Civil_Comercial_Estruturas_Custos_Obras.csv",
    ])
    rota = opcao.strip()[0:2].strip().rstrip(".")

    arquivo = st.file_uploader("Arraste o CSV padronizado aqui", type=["csv"])

    if arquivo:
        try:
            df = pd.read_csv(arquivo, sep=";", encoding="utf-8-sig", dtype=str)
            st.success(f"✅ {df.shape[0]} linhas | Colunas: {list(df.columns)}")

            if st.button("🚀 Importar", type="primary"):
                with st.spinner("Processando..."):
                    try:

                        # ── ROTA 1: EQUIPE ──────────────────────────────────
                        # Colunas: nome, email, setor, status
                        if rota == "1":
                            df = nulos(df)
                            pacote = df[["nome","email","setor","status"]].to_dict("records")
                            supabase.table("equipe").insert(pacote).execute()
                            st.success(f"🎉 {len(pacote)} colaboradores importados!")

                        # ── ROTA 2: OBRAS ────────────────────────────────────
                        # Colunas: codigo, nome, status
                        elif rota == "2":
                            df = nulos(df)
                            pacote = df[["codigo","nome","status"]].to_dict("records")
                            supabase.table("obras").upsert(pacote, on_conflict="nome").execute()
                            st.success(f"🎉 {len(pacote)} obras importadas!")

                        # ── ROTA 3: TEMPLATE JORNADA ─────────────────────────
                        # Colunas: item, etapa, descricao, status_padrao
                        elif rota == "3":
                            df["item"] = pd.to_numeric(df["item"], errors="coerce")
                            df = nulos(df)
                            pacote = df[["item","etapa","descricao","status_padrao"]].to_dict("records")
                            pacote = fix_ids(pacote)
                            supabase.table("template_jornada").insert(pacote).execute()
                            st.success(f"🎉 {len(pacote)} itens do template importados!")

                        # ── ROTA 4: ROTINAS ──────────────────────────────────
                        # Colunas: setor, frequencia, atividade
                        elif rota == "4":
                            df = nulos(df)
                            pacote = df[["setor","frequencia","atividade"]].to_dict("records")
                            supabase.table("rotinas").insert(pacote).execute()
                            st.success(f"🎉 {len(pacote)} rotinas importadas!")

                        # ── ROTA 5: EXTRAS ───────────────────────────────────
                        # Colunas: colaborador_nome, origem, descricao, status,
                        #          prazo, entrega_real, observacao
                        elif rota == "5":
                            meq = mapa_equipe()
                            df["colaborador_id"] = df["colaborador_nome"].apply(normalize).map(meq)
                            df["prazo"]        = formatar_data(df["prazo"])
                            df["entrega_real"] = formatar_data(df["entrega_real"])
                            ignorados = df["colaborador_id"].isna().sum()
                            df = nulos(df.dropna(subset=["colaborador_id"]))
                            pacote = df[["colaborador_id","origem","descricao",
                                         "status","prazo","entrega_real","observacao"]].to_dict("records")
                            pacote = fix_ids(pacote)
                            supabase.table("extras").insert(pacote).execute()
                            st.success(f"🎉 {len(pacote)} extras importados!")
                            if ignorados: st.warning(f"⚠️ {ignorados} linha(s) ignorada(s): colaborador não encontrado.")

                        # ── ROTA 6: OBRAS TAREFAS ────────────────────────────
                        # Colunas: obra_codigo, item, etapa, descricao,
                        #          R, A, C, I, status,
                        #          inicio_previsto, entrega_prevista, entrega_real, observacoes
                        elif rota == "6":
                            mob = mapa_obras()
                            meq = mapa_equipe()
                            pacote, ignorados = [], 0

                            for _, row in df.iterrows():
                                cod = extrair_codigo(row.get("obra_codigo"))
                                obra_id = mob.get(cod) if cod else None
                                if not obra_id:
                                    ignorados += 1; continue

                                def eq_id(col):
                                    return meq.get(normalize(row.get(col)))

                                pacote.append({
                                    "obra_id":          int(obra_id),
                                    "item":             clean_str(row.get("item")),
                                    "etapa":            clean_str(row.get("etapa")),
                                    "descricao":        clean_str(row.get("descricao")),
                                    "responsavel_id":   int(eq_id("R")) if eq_id("R") else None,
                                    "aprovador_id":     int(eq_id("A")) if eq_id("A") else None,
                                    "consultado_id":    int(eq_id("C")) if eq_id("C") else None,
                                    "informado_id":     int(eq_id("I")) if eq_id("I") else None,
                                    "status":           clean_str(row.get("status")) or "A Iniciar",
                                    "inicio_previsto":  formatar_data_valor(row.get("inicio_previsto")),
                                    "entrega_prevista": formatar_data_valor(row.get("entrega_prevista")),
                                    "entrega_real":     formatar_data_valor(row.get("entrega_real")),
                                    "observacoes":      clean_str(row.get("observacoes")),
                                })

                            if pacote:
                                supabase.table("obras_tarefas").insert(pacote).execute()
                                st.success(f"🎉 {len(pacote)} tarefas importadas!")
                                if ignorados: st.warning(f"⚠️ {ignorados} linha(s) ignorada(s): obra não encontrada.")
                            else:
                                st.error("❌ Nenhuma tarefa válida. Verifique a coluna obra_codigo.")

                        # ── ROTAS 7, 8, 9: PRODUÇÃO ──────────────────────────
                        elif rota in ["7","8","9"]:
                            mob = mapa_obras()
                            df["obra_id"] = aplicar_obra_id(df["obra_codigo"].apply(extrair_codigo), mob)
                            ignorados = df["obra_id"].isna().sum()
                            df = df.dropna(subset=["obra_id"])

                            if rota == "7":
                                tabela = "producao_fabricacao"
                                num_cols = ["qtde_pecas","volume_total","volume_teorico",
                                            "peso_aco","peso_aco_frouxo","peso_aco_protendido","comprimento"]
                                date_col = "data_fabricacao"
                                cols_bd  = ["obra_id","peca","codigo","etapa","produto","secao",
                                            "qtde_pecas","volume_total","data_fabricacao",
                                            "volume_teorico","peso_aco","peso_aco_frouxo",
                                            "peso_aco_protendido","comprimento"]
                            elif rota == "8":
                                tabela = "producao_transporte"
                                num_cols = ["volume_real","peso","numero_carga"]
                                date_col = "data_expedicao"
                                cols_bd  = ["obra_id","peca","codigo","etapa","produto",
                                            "data_expedicao","volume_real","status","peso",
                                            "numero_carga","transportadora","motorista","nota_fiscal"]
                            else:
                                tabela = "producao_montagem"
                                num_cols = ["qtde_pecas","volume_total","volume_teorico","peso"]
                                date_col = "data_montagem"
                                cols_bd  = ["obra_id","peca","codigo","etapa","produto","secao",
                                            "qtde_pecas","volume_total","data_montagem",
                                            "volume_teorico","peso"]

                            df[date_col] = formatar_data(df[date_col])
                            for c in num_cols:
                                if c in df.columns: df[c] = formatar_numero(df[c])
                            df = nulos(df)
                            pacote = df[cols_bd].to_dict("records")
                            pacote = fix_ids(pacote)
                            total = enviar_lotes(tabela, pacote, f"Enviando para {tabela}...")
                            st.success(f"🎉 {total} registros importados em {tabela}!")
                            if ignorados: st.warning(f"⚠️ {ignorados} linha(s) ignorada(s): obra não encontrada.")

                        # ── ROTA 10: CUSTOS ──────────────────────────────────
                        # (mantido abaixo; rotas 11 e 12 adicionadas ao final)
                        # CSV já vem pré-processado (datas e números convertidos)
                        elif rota == "10":
                            mob = mapa_obras()
                            df["obra_id"] = aplicar_obra_id(df["obra_codigo"].apply(extrair_codigo), mob)
                            df = df.replace("", None)
                            df = df.where(pd.notnull(df), None)
                            com_obra = df["obra_id"].notna().sum()
                            sem_obra = df["obra_id"].isna().sum()
                            cols_bd = ["obra_id","data","id_lancamento","numero_doc",
                                       "centro_custos","conta_macro","conta_gerencial",
                                       "cli_fornecedor","produto_servico","criado_por",
                                       "valor_global","qtd","preco_unitario","origem",
                                       "chave_coligada_id_origem","cod_tipo_doc_movimento"]
                            for col in ["valor_global","qtd","preco_unitario"]:
                                df[col] = pd.to_numeric(df[col], errors="coerce")
                                df[col] = df[col].where(pd.notnull(df[col]), None)
                            import math as _m
                            pacote_limpo = []
                            for row in df[cols_bd].to_dict("records"):
                                pacote_limpo.append({
                                    k: None if (
                                        v is None
                                        or (isinstance(v, float) and (_m.isnan(v) or _m.isinf(v)))
                                        or str(v).strip().lower() in ("nan","nat","none","inf","")
                                    ) else v
                                    for k, v in row.items()
                                })
                            total = enviar_lotes("custos", pacote_limpo, "Enviando custos...")
                            st.success(f"🎉 {total} registros importados!")
                            st.info(f"✅ {com_obra} custos diretos (com obra) | 🏭 {sem_obra} custos indiretos (sem obra)")

                        # ── ROTA 11: NOTAS FISCAIS / MEDIÇÕES ────────────────
                        # Fonte: Lista_Medição.csv — colunas mapeadas por posição
                        elif rota == "11":
                            arquivo.seek(0)
                            df11 = pd.read_csv(arquivo, sep=None, engine="python",
                                               encoding="utf-8-sig", dtype=str, header=0)

                            # Renomear por posição — imune a acentos no cabeçalho
                            nomes11 = [
                                "codigo_obra_original", "titulo", "etapa_obra",
                                "data_acao", "nome_pagador", "cnpj_pagador",
                                "numero_nf", "numero_nf_remessa", "data_vencimento",
                                "descricao", "valor", "cnpj_recebedor",
                                "razao_social_recebedor", "tipo", "observacoes", "categoria",
                            ]
                            df11 = df11.rename(columns={
                                df11.columns[i]: nomes11[i]
                                for i in range(min(len(df11.columns), len(nomes11)))
                            })

                            # Ignorar NFs canceladas antes de qualquer processamento
                            mask_cancel = (df11["tipo"].str.strip().str.upper()
                                           == "NOTA FISCAL CANCELADA")
                            n_canceladas = int(mask_cancel.sum())
                            df11 = df11[~mask_cancel].copy()

                            # Converter datas DD/MM/YYYY → YYYY-MM-DD
                            df11["data_acao"]       = formatar_data(df11["data_acao"])
                            df11["data_vencimento"] = formatar_data(df11["data_vencimento"])

                            # Limpar valor monetário → float ou None
                            df11["valor"] = formatar_numero(df11["valor"])

                            # obra_id via código 4 dígitos na coluna titulo (pode ser NULL)
                            mob = mapa_obras()
                            df11["obra_id"] = aplicar_obra_id(
                                df11["titulo"].apply(extrair_codigo), mob)
                            sem_obra11 = int(df11["obra_id"].isna().sum())

                            # Colunas que existem no banco (schema notas_fiscais)
                            cols_bd11 = [
                                "obra_id", "codigo_obra_original", "titulo",
                                "etapa_obra", "data_acao", "nome_pagador",
                                "cnpj_pagador", "numero_nf", "numero_nf_remessa",
                                "data_vencimento", "descricao", "valor",
                                "cnpj_recebedor", "razao_social_recebedor",
                                "tipo", "observacoes", "categoria",
                            ]
                            cols_bd11 = [c for c in cols_bd11 if c in df11.columns]
                            pacote11 = df11[cols_bd11].to_dict("records")
                            pacote11 = fix_ids(pacote11)
                            pacote11 = limpar_nan_pacote(pacote11)  # ← purga NaN residuais
                            total11 = enviar_lotes("notas_fiscais", pacote11,
                                                   "Enviando notas fiscais...")
                            st.success(f"🎉 {total11} NFs importadas!")
                            st.info(
                                f"✅ {total11 - sem_obra11} com obra vinculada  "
                                f"| ⚠️ {sem_obra11} sem obra  "
                                f"| 🚫 {n_canceladas} canceladas ignoradas")

                        # ── ROTA 12: RESUMO FINANCEIRO OBRAS ─────────────────
                        # Fonte: Civil_Comercial_Estruturas_Custos_Obras.csv
                        elif rota == "12":
                            arquivo.seek(0)
                            df12 = pd.read_csv(arquivo, sep=None, engine="python",
                                               encoding="utf-8-sig", dtype=str, header=0)

                            # Renomear por posição
                            nomes12 = [
                                "data_contrato", "codigo_produto", "obra_nome",
                                "volume", "faturamento_total", "faturamento_civil",
                                "faturamento_direto", "custo_total", "despesas_indiretas",
                                "impostos", "lucro", "cimento_cliente_ton", "cimento_civil_ton",
                                "chave_coligada", "razao_social", "cnpj", "responsavel",
                                "email", "volume_projeto", "concreto", "aco_estrutural",
                                "formas", "mo_producao", "eps", "estuque", "projetos",
                                "descida_agua", "insertos", "consoles", "investimentos",
                                "neoprene", "materiais_consumo", "equip_fab",
                                "custos_indiretos", "pecas_consorcio", "frete",
                                "equip_montagem", "mo_montagem", "despesas_equipe",
                                "topografia", "mobilizacao", "equip_aux_montagem",
                                "outros", "eventuais", "despesas_comerciais",
                            ]
                            df12 = df12.rename(columns={
                                df12.columns[i]: nomes12[i]
                                for i in range(min(len(df12.columns), len(nomes12)))
                            })

                            # Converter data
                            df12["data_contrato"] = formatar_data(df12["data_contrato"])

                            # Limpar todos os campos numéricos
                            texto12 = {"data_contrato", "codigo_produto", "obra_nome",
                                       "chave_coligada", "razao_social", "cnpj",
                                       "responsavel", "email"}
                            for c in [col for col in nomes12
                                      if col not in texto12 and col in df12.columns]:
                                df12[c] = formatar_numero(df12[c])

                            # obra_id via código 4 dígitos em obra_nome (pode ser NULL)
                            mob = mapa_obras()
                            df12["obra_id"] = aplicar_obra_id(
                                df12["obra_nome"].apply(extrair_codigo), mob)
                            sem_obra12 = int(df12["obra_id"].isna().sum())

                            # Somente colunas que existem no schema obras_financeiro
                            cols_bd12 = [
                                "obra_id", "data_contrato", "codigo_produto", "obra_nome",
                                "chave_coligada", "razao_social", "cnpj", "responsavel",
                                "email", "volume", "volume_projeto", "faturamento_total",
                                "faturamento_civil", "faturamento_direto", "custo_total",
                                "despesas_indiretas", "impostos", "lucro", "concreto",
                                "aco_estrutural", "formas", "mo_producao", "eps", "estuque",
                                "projetos", "descida_agua", "insertos", "consoles",
                                "investimentos", "neoprene", "materiais_consumo", "equip_fab",
                                "custos_indiretos", "pecas_consorcio", "frete",
                                "equip_montagem", "mo_montagem", "despesas_equipe",
                                "topografia", "mobilizacao", "equip_aux_montagem",
                                "outros", "eventuais", "despesas_comerciais",
                                "cimento_cliente_ton", "cimento_civil_ton",
                            ]
                            cols_bd12 = [c for c in cols_bd12 if c in df12.columns]
                            pacote12 = df12[cols_bd12].to_dict("records")
                            pacote12 = fix_ids(pacote12)
                            pacote12 = limpar_nan_pacote(pacote12)  # ← purga NaN residuais
                            supabase.table("obras_financeiro").insert(pacote12).execute()
                            st.success(
                                f"🎉 {len(pacote12)} registros importados em obras_financeiro!")
                            st.info(
                                f"✅ {len(pacote12) - sem_obra12} com obra vinculada  "
                                f"| ⚠️ {sem_obra12} sem obra")

                    except Exception as e:
                        st.error(f"❌ Erro no banco: {e}")
        except Exception as e:
            st.error(f"❌ Erro na leitura do CSV: {e}")

# ==========================================================
# EQUIPE
# ==========================================================
elif pagina_selecionada == "👥 Equipe":
    st.header("👥 Gestão da Equipe")

    @st.cache_data(ttl=60)
    def _carregar_equipe_completa():
        return supabase.table("equipe").select("*").order("nome").execute().data

    df_eq = pd.DataFrame(_carregar_equipe_completa())
    aba1, aba2 = st.tabs(["👁️ Editar", "➕ Novo"])

    with aba1:
        if not df_eq.empty:
            st.info("Clique duplo para editar. Selecione a linha + Delete para excluir.")
            editado = st.data_editor(df_eq, use_container_width=True, num_rows="dynamic",
                                      disabled=["id","created_at"], key="ed_equipe")
            if st.button("💾 Salvar", type="primary"):
                m = st.session_state["ed_equipe"]
                for i in m.get("deleted_rows", []):
                    supabase.table("equipe").delete().eq("id", df_eq.iloc[i]["id"]).execute()
                for i, ed in m.get("edited_rows", {}).items():
                    supabase.table("equipe").update(ed).eq("id", df_eq.iloc[i]["id"]).execute()
                _carregar_equipe_completa.clear()
                carregar_equipe_ativa.clear()
                st.success("Salvo!"); st.rerun()
        else:
            st.warning("Nenhum colaborador.")

    with aba2:
        with st.form("novo_colab", clear_on_submit=True):
            c1, c2 = st.columns(2)
            nome  = c1.text_input("Nome *")
            email = c2.text_input("E-mail")
            setor = c1.selectbox("Setor", ["Engenharia","Administrativo","Produção",
                                            "Montagem","Projetos","Planejamento (PCP)",
                                            "Controller","Comercial","Manutenção"])
            status = c2.selectbox("Status", ["Ativo","Inativo"])
            if st.form_submit_button("💾 Cadastrar", type="primary") and nome:
                supabase.table("equipe").insert({"nome":nome,"email":email,
                                                  "setor":setor,"status":status}).execute()
                _carregar_equipe_completa.clear()
                carregar_equipe_ativa.clear()
                st.rerun()

# ==========================================================
# FUNÇÕES AUXILIARES DA SÚMULA
# ==========================================================

@st.cache_data(ttl=60)
def carregar_obras_ativas():
    resp = supabase.table("obras")\
        .select("id, codigo, nome, status, modalidade, cliente, responsavel_id")\
        .order("nome").execute()
    return resp.data

@st.cache_data(ttl=60)
def carregar_equipe_ativa():
    resp = supabase.table("equipe").select("id, nome").eq("status","Ativo").order("nome").execute()
    return resp.data

@st.cache_data(ttl=60)
def carregar_template():
    resp = supabase.table("template_jornada").select("*").order("item").execute()
    return resp.data

@st.cache_data(ttl=60)
def carregar_tarefas(obra_id):
    resp = supabase.table("obras_tarefas")\
        .select("id, item, etapa, descricao, status, observacoes, impedimento, "
                "avanco_percent, inicio_previsto, entrega_prevista, entrega_real, "
                "gut_gravidade, gut_urgencia, gut_tendencia, gut_score, "
                "responsavel_id, aprovador_id, consultado_id, informado_id, "
                "responsavel:equipe!obras_tarefas_responsavel_id_fkey(nome), "
                "aprovador:equipe!obras_tarefas_aprovador_id_fkey(nome)")\
        .eq("obra_id", obra_id)\
        .execute()
    return resp.data

@st.cache_data(ttl=60)
def carregar_alertas():
    from datetime import date, timedelta
    hoje  = date.today()
    limit = hoje + timedelta(days=2)
    try:
        resp = supabase.table("obras_tarefas")\
            .select("descricao, entrega_prevista, status, obras(nome)")\
            .not_.in_("status",["Concluído","N/A"])\
            .lte("entrega_prevista", limit.isoformat())\
            .gte("entrega_prevista", hoje.isoformat())\
            .execute()
        return resp.data
    except: return []

from datetime import date as _date_cls

def calcular_farol(entrega_prevista, status, _hoje=None):
    if status in ("Concluído","N/A"): return "✅"
    if not entrega_prevista: return "⚪"
    hoje = _hoje or _date_cls.today()
    try: diff = (_date_cls.fromisoformat(str(entrega_prevista)) - hoje).days
    except: return "⚪"
    return "🔴" if diff < 0 else ("🟡" if diff <= 2 else "🟢")

def calcular_desvio(entrega_prevista, entrega_real, status, _hoje=None):
    if not entrega_prevista: return "—"
    try: prev = _date_cls.fromisoformat(str(entrega_prevista))
    except: return "—"
    if entrega_real:
        try: diff = (_date_cls.fromisoformat(str(entrega_real)) - prev).days
        except: return "—"
    else:
        if status in ("Concluído","N/A"): return "—"
        hoje = _hoje or _date_cls.today()
        diff = (hoje - prev).days
    if diff == 0: return "✅ 0d"
    return f"🔴 +{diff}d" if diff > 0 else f"🟢 {diff}d"

def gut_emoji(score):
    s = score or 1
    if s >= 75: return f"🔴 {s}"
    elif s >= 27: return f"🟡 {s}"
    return f"🟢 {s}"

def parse_date(val):
    if not val: return None
    try:
        from datetime import date
        return date.fromisoformat(str(val))
    except: return None

@st.cache_data(ttl=120)
def carregar_ultimo_update():
    """Busca o último created_at por obra sem varrer toda a tabela.
    Limit de 1000 cobre até ~100 obras com folga e é ordens de grandeza
    mais rápido do que trazer todos os registros."""
    try:
        upd = supabase.table("obras_tarefas").select("obra_id, created_at")\
            .order("created_at", desc=True).limit(1000).execute().data
        resultado = {}
        for u in upd:
            oid = u["obra_id"]
            if oid not in resultado:
                resultado[oid] = u.get("created_at", "")[:10]
        return resultado
    except:
        return {}

@st.cache_data(ttl=60)
def carregar_tarefas_colab(colab_id):
    """Busca todas as tarefas onde o colaborador aparece em qualquer papel RACI."""
    resp = supabase.table("obras_tarefas")\
        .select("id, item, etapa, descricao, status, observacoes, impedimento, "
                "avanco_percent, inicio_previsto, entrega_prevista, entrega_real, "
                "gut_gravidade, gut_urgencia, gut_tendencia, gut_score, obra_id, origem, "
                "responsavel_id, aprovador_id, consultado_id, informado_id, "
                "obras(nome, codigo)")\
        .or_(f"responsavel_id.eq.{colab_id},aprovador_id.eq.{colab_id},"
             f"consultado_id.eq.{colab_id},informado_id.eq.{colab_id}")\
        .execute()
    return resp.data

def limpar_cache():
    carregar_obras_ativas.clear()
    carregar_equipe_ativa.clear()
    carregar_template.clear()
    carregar_tarefas.clear()
    carregar_alertas.clear()
    carregar_ultimo_update.clear()
    carregar_tarefas_colab.clear()

@st.cache_data(ttl=60)
def volume_referencia(obra_id):
    """Retorna o volume de referência da obra para uso financeiro.
    Prioridade: volume_projeto > volume (comercial) > None."""
    try:
        resp = supabase.table("obras_financeiro")\
            .select("volume_projeto, volume")\
            .eq("obra_id", obra_id)\
            .limit(1).execute()
        if not resp.data:
            return None
        row = resp.data[0]
        if row.get("volume_projeto") not in (None, ""):
            return float(row["volume_projeto"])
        if row.get("volume") not in (None, ""):
            return float(row["volume"])
        return None
    except Exception:
        return None

# ==========================================================
# PÁGINA: GESTÃO DE OBRAS (SÚMULA)
# ==========================================================
if pagina_selecionada == "🏗️ Gestão de Obras":
    import pandas as pd
    from datetime import date

    STATUS_OPCOES = ["A Iniciar","Em Andamento","Impedido","Concluído","N/A"]

    if "fechar_alerta" not in st.session_state:
        st.session_state["fechar_alerta"] = False

    obras        = carregar_obras_ativas()
    equipe_lista = carregar_equipe_ativa()
    equipe_nomes = [e["nome"] for e in equipe_lista]
    equipe_ids   = {e["nome"]: e["id"] for e in equipe_lista}

    if not obras: st.error("Nenhuma obra cadastrada."); st.stop()

    # ── ALERTA COMPACTO ───────────────────────────────────
    if not st.session_state["fechar_alerta"]:
        alertas = carregar_alertas()
        if alertas:
            a1, a2 = st.columns([11,1])
            with a1:
                with st.expander(f"⚠️ {len(alertas)} tarefa(s) vencem em até 2 dias", expanded=False):
                    for a in alertas:
                        obra_nome = (a.get("obras") or {}).get("nome","?")
                        st.caption(f"📌 {obra_nome} · {a['descricao']} · `{a.get('entrega_prevista','?')}`")
            with a2:
                if st.button("✕", key="btn_fechar_alerta"):
                    st.session_state["fechar_alerta"] = True
                    st.rerun()

    # ── SELEÇÃO DE OBRA ───────────────────────────────────
    r1,r2,r3 = st.columns([1,3,1])
    status_disponiveis = sorted(set(o.get("status") or "—" for o in obras))
    filtro_status_obra = r1.selectbox("Status",["Todos"]+status_disponiveis,
        key="filt_obra_status", label_visibility="collapsed")

    obras_filtradas = obras if filtro_status_obra == "Todos" \
        else [o for o in obras if o.get("status") == filtro_status_obra]
    if not obras_filtradas: st.warning("Nenhuma obra."); st.stop()

    ultimo_upd = carregar_ultimo_update()

    def label_obra(o):
        upd_str = ultimo_upd.get(o["id"])
        return f"{o['codigo']} — {o['nome']}" + (f" · 🕐{upd_str}" if upd_str else "")

    opcoes_obras = {label_obra(o): o["id"] for o in obras_filtradas}
    obra_label   = r2.selectbox("Obra", list(opcoes_obras.keys()),
        key="sel_obra", label_visibility="collapsed")
    obra_id = opcoes_obras[obra_label]

    if r3.button("🔄 Atualizar"):
        limpar_cache(); st.rerun()

    # ── EDITAR DADOS DA OBRA ──────────────────────────────
    obra_atual = next((o for o in obras if o["id"] == obra_id), {})

    STATUS_OBRA  = ["Em Andamento", "Concluída", "Cancelada", "Proposta"]
    MODALIDADES  = ["FOB", "CIF", "Montagem", "Não definida"]

    with st.expander("⚙️ Editar dados da obra", expanded=False):
        ea1, ea2, ea3, ea4 = st.columns(4)

        cur_status = obra_atual.get("status") or "Em Andamento"
        idx_status = STATUS_OBRA.index(cur_status) if cur_status in STATUS_OBRA else 0
        ed_status = ea1.selectbox("Status da obra", STATUS_OBRA,
                                   index=idx_status, key="ed_obra_status")

        cur_modal = obra_atual.get("modalidade") or "Não definida"
        idx_modal = MODALIDADES.index(cur_modal) if cur_modal in MODALIDADES else 3
        ed_modal = ea2.selectbox("Modalidade", MODALIDADES,
                                  index=idx_modal, key="ed_obra_modal")

        ed_cliente = ea3.text_input("Cliente", value=obra_atual.get("cliente") or "",
                                     key="ed_obra_cliente")

        # Responsável — lista da equipe com opção vazia
        resp_opcoes = ["—"] + equipe_nomes
        cur_resp_id = obra_atual.get("responsavel_id")
        cur_resp_nome = next((e["nome"] for e in equipe_lista
                              if e["id"] == cur_resp_id), None)
        idx_resp = resp_opcoes.index(cur_resp_nome) if cur_resp_nome in resp_opcoes else 0
        ed_resp = ea4.selectbox("Responsável", resp_opcoes,
                                 index=idx_resp, key="ed_obra_resp")

        if st.button("💾 Salvar dados da obra", key="btn_salvar_obra"):
            novo_resp_id = equipe_ids.get(ed_resp) if ed_resp != "—" else None
            payload = {
                "status":         ed_status,
                "modalidade":     ed_modal,
                "cliente":        ed_cliente.strip() or None,
                "responsavel_id": int(novo_resp_id) if novo_resp_id else None,
            }
            supabase.table("obras").update(payload).eq("id", obra_id).execute()
            limpar_cache()
            st.success("✅ Dados da obra atualizados!")
            st.rerun()

    # ── CARREGA E PREPARA DADOS ───────────────────────────
    tarefas = carregar_tarefas(obra_id)

    # métricas
    if tarefas:
        total      = len(tarefas)
        concluidas = sum(1 for t in tarefas if t["status"] == "Concluído")
        atrasadas  = sum(1 for t in tarefas if calcular_farol(t.get("entrega_prevista"), t["status"]) == "🔴")
        impedidas  = sum(1 for t in tarefas if t["status"] == "Impedido")
        pct        = int(concluidas / total * 100) if total else 0
        st.progress(pct/100)
        st.caption(f"**{pct}% concluído** · 📋 {total} · ✅ {concluidas} · 🔴 {atrasadas} · 🚧 {impedidas}")

    # ── FILTROS ───────────────────────────────────────────
    etapas_disp = sorted(set(t.get("etapa") or "—" for t in tarefas if t.get("etapa")))
    fc1,fc2,fc3,fc4,fc5 = st.columns([2,2,2,2,1])
    f_etapa  = fc1.selectbox("Etapa",  ["Todas"]+etapas_disp,    key="f_etapa",   label_visibility="collapsed")
    f_status = fc2.selectbox("Status", ["Todos"]+STATUS_OPCOES,  key="f_fstatus", label_visibility="collapsed")
    f_resp   = fc3.selectbox("Resp.",  ["Todos"]+equipe_nomes,   key="f_resp",    label_visibility="collapsed")
    f_gut    = fc4.selectbox("GUT",    ["Todos","🔴 Alto","🟡 Médio","🟢 Baixo"], key="f_gut", label_visibility="collapsed")
    f_conc   = fc5.checkbox("✅+N/A",  value=False, key="f_conc")

    # ── NOVA TAREFA ───────────────────────────────────────
    with st.expander("➕ Nova tarefa", expanded=False):
        template        = carregar_template()
        etapas_template = sorted(set(t.get("etapa","") for t in template if t.get("etapa")))
        etapas_obra     = sorted(set(t.get("etapa","") for t in tarefas  if t.get("etapa")))
        etapas_combo    = sorted(set(etapas_template + etapas_obra))
        desc_por_etapa  = {}
        for t in template + tarefas:
            ep = t.get("etapa","")
            desc_por_etapa.setdefault(ep,[]).append(t.get("descricao",""))

        na1,na2,na3 = st.columns(3)
        n_etapa_sel = na1.selectbox("Etapa",["(nova)"]+etapas_combo, key="n_etapa_sel")
        n_etapa     = na1.text_input("Nome da etapa", key="n_etapa_livre") \
                      if n_etapa_sel == "(nova)" else n_etapa_sel
        n_item      = na2.text_input("Item", key="n_item")
        n_status    = na3.selectbox("Status", STATUS_OPCOES, key="n_status")

        descs = sorted(set(desc_por_etapa.get(n_etapa,[]))) if n_etapa_sel != "(nova)" else []
        if descs:
            n_desc_sel = st.selectbox("Descrição sugerida",["(escrever)"]+descs, key="n_desc_sel")
            n_desc = st.text_input("Descrição", key="n_desc_livre") \
                     if n_desc_sel == "(escrever)" else n_desc_sel
        else:
            n_desc = st.text_input("Descrição", key="n_desc_livre2")
        n_obs = st.text_area("Observação", key="n_obs", height=50)

        nr1,nr2,nr3,nr4 = st.columns(4)
        n_r = nr1.selectbox("R", equipe_nomes, key="n_r")
        n_a = nr2.selectbox("A",["—"]+equipe_nomes, key="n_a")
        n_c = nr3.selectbox("C",["—"]+equipe_nomes, key="n_c")
        n_i = nr4.selectbox("I",["—"]+equipe_nomes, key="n_i")

        nd1,nd2 = st.columns(2)
        n_inicio  = nd1.date_input("Início",  value=None, key="n_ini", format="DD/MM/YYYY")
        n_entrega = nd2.date_input("Entrega", value=None, key="n_ent", format="DD/MM/YYYY")

        ng1,ng2,ng3,ng4 = st.columns(4)
        n_g = ng1.slider("G",1,5,1,key="n_g")
        n_u = ng2.slider("U",1,5,1,key="n_u")
        n_t = ng3.slider("T",1,5,1,key="n_t")
        ng4.metric("GUT", n_g*n_u*n_t)

        if st.button("💾 Criar", type="primary", key="btn_nova"):
            if n_desc:
                supabase.table("obras_tarefas").insert({
                    "obra_id":          obra_id,
                    "item":             n_item or None,
                    "etapa":            n_etapa or None,
                    "descricao":        n_desc,
                    "observacoes":      n_obs or None,
                    "responsavel_id":   equipe_ids.get(n_r),
                    "aprovador_id":     equipe_ids.get(n_a) if n_a != "—" else None,
                    "consultado_id":    equipe_ids.get(n_c) if n_c != "—" else None,
                    "informado_id":     equipe_ids.get(n_i) if n_i != "—" else None,
                    "status":           n_status,
                    "inicio_previsto":  n_inicio.isoformat()  if n_inicio  else None,
                    "entrega_prevista": n_entrega.isoformat() if n_entrega else None,
                    "gut_gravidade":    n_g,"gut_urgencia":n_u,"gut_tendencia":n_t,
                    "gut_score":        n_g*n_u*n_t,"avanco_percent":0,
                }).execute()
                limpar_cache(); st.success("✅ Criada!"); st.rerun()
            else:
                st.warning("Descrição é obrigatória.")

    # ── SEM TAREFAS ───────────────────────────────────────
    if not tarefas:
        st.info("Esta obra ainda não possui tarefas.")
        template = carregar_template()
        if template and st.button("📋 Criar do Template", type="primary"):
            supabase.table("obras_tarefas").insert([{
                "obra_id":obra_id,"item":str(t.get("item","")),
                "etapa":t.get("etapa"),"descricao":t.get("descricao"),
                "status":t.get("status_padrao") or "A Iniciar",
                "gut_gravidade":1,"gut_urgencia":1,"gut_tendencia":1,
                "gut_score":1,"avanco_percent":0,
            } for t in template]).execute()
            limpar_cache(); st.rerun()
        st.stop()

    # ── APLICA FILTROS ────────────────────────────────────
    tf = tarefas
    if not f_conc:          tf = [t for t in tf if t["status"] not in ("Concluído","N/A")]
    if f_etapa  != "Todas": tf = [t for t in tf if t.get("etapa") == f_etapa]
    if f_status != "Todos": tf = [t for t in tf if t["status"] == f_status]
    if f_resp   != "Todos": tf = [t for t in tf if (t.get("responsavel") or {}).get("nome") == f_resp]
    if "Alto"   in f_gut:   tf = [t for t in tf if (t.get("gut_score") or 1) >= 75]
    elif "Médio" in f_gut:  tf = [t for t in tf if 27 <= (t.get("gut_score") or 1) < 75]
    elif "Baixo" in f_gut:  tf = [t for t in tf if (t.get("gut_score") or 1) < 27]

    # ── MONTA DATAFRAME ───────────────────────────────────
    _hoje = date.today()   # calculado uma única vez para todo o loop
    rows = []
    for t in tf:
        rows.append({
            "_id":        t["id"],
            "GUT":        gut_emoji(t.get("gut_score") or 1),
            "Etapa":      t.get("etapa") or "—",
            "Descrição":  f"{calcular_farol(t.get('entrega_prevista'), t['status'], _hoje)} {t.get('descricao') or '—'}",
            "Resp.":      (t.get("responsavel") or {}).get("nome","—"),
            "Status":     t["status"],
            "Desvio":     calcular_desvio(t.get("entrega_prevista"), t.get("entrega_real"), t["status"], _hoje),
            "Av.%":       t.get("avanco_percent") or 0,
        })

    df = pd.DataFrame(rows)
    st.caption(f"**{len(df)}** tarefa(s) · clique em uma linha para editar")

    # ── TABELA NATIVA ─────────────────────────────────────
    sel = st.dataframe(
        df.drop(columns=["_id"]),
        use_container_width=True,
        hide_index=True,
        height=min(400, 36 + 35 * len(df)),
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "GUT":       st.column_config.TextColumn("GUT",      width="small"),
            "Etapa":     st.column_config.TextColumn("Etapa",    width="medium"),
            "Descrição": st.column_config.TextColumn("Descrição",width="large"),
            "Resp.":     st.column_config.TextColumn("Resp.",    width="medium"),
            "Status":    st.column_config.TextColumn("Status",   width="medium"),
            "Desvio":    st.column_config.TextColumn("Desvio",   width="small"),
            "Av.%":      st.column_config.ProgressColumn("Av.%", min_value=0, max_value=100, width="small"),
        }
    )

    # ── FORMULÁRIO DE EDIÇÃO ──────────────────────────────
    linhas_sel = sel.selection.rows if sel.selection else []
    if linhas_sel:
        idx = linhas_sel[0]
        t   = tf[idx]
        st.divider()
        st.markdown(f"**✏️ Editando:** {t.get('descricao','')}")

        resp_nome = (t.get("responsavel") or {}).get("nome","—")

        ea,eb,ec = st.columns(3)
        r_nome = ea.selectbox("R — Responsável", equipe_nomes,
            index=equipe_nomes.index(resp_nome) if resp_nome in equipe_nomes else 0,
            key=f"r_{t['id']}")
        a_nome = eb.selectbox("A — Aprovador",  ["—"]+equipe_nomes, key=f"a_{t['id']}")
        c_nome = ec.selectbox("C — Consultado", ["—"]+equipe_nomes, key=f"c_{t['id']}")
        i_nome = ea.selectbox("I — Informado",  ["—"]+equipe_nomes, key=f"i_{t['id']}")

        novo_status = eb.selectbox("Status", STATUS_OPCOES,
            index=STATUS_OPCOES.index(t["status"]) if t["status"] in STATUS_OPCOES else 0,
            key=f"st_{t['id']}")
        novo_avanco = ec.slider("% Avanço",0,100,t.get("avanco_percent") or 0,
            step=5, key=f"av_{t['id']}")

        novo_imp = t.get("impedimento") or ""
        if novo_status == "Impedido":
            novo_imp = st.text_input("🚧 Impedimento", value=novo_imp, key=f"imp_{t['id']}")

        fd1,fd2,fd3 = st.columns(3)
        novo_inicio  = fd1.date_input("Início",  value=parse_date(t.get("inicio_previsto")),  key=f"ini_{t['id']}",  format="DD/MM/YYYY")
        novo_entrega = fd2.date_input("Entrega", value=parse_date(t.get("entrega_prevista")), key=f"ent_{t['id']}",  format="DD/MM/YYYY")
        novo_real    = fd3.date_input("Real",    value=parse_date(t.get("entrega_real")),     key=f"real_{t['id']}", format="DD/MM/YYYY")

        gg1,gg2,gg3,gg4 = st.columns(4)
        ng = gg1.slider("G",1,5,t.get("gut_gravidade") or 1, key=f"g_{t['id']}")
        nu = gg2.slider("U",1,5,t.get("gut_urgencia")  or 1, key=f"u_{t['id']}")
        nt = gg3.slider("T",1,5,t.get("gut_tendencia") or 1, key=f"te_{t['id']}")
        gg4.metric("GUT", ng*nu*nt)

        novo_obs = st.text_area("Obs.", value=t.get("observacoes") or "",
            key=f"obs_{t['id']}", height=50)

        s1,s2 = st.columns([1,5])
        if s1.button("💾 Salvar", type="primary", key=f"save_{t['id']}"):
            supabase.table("obras_tarefas").update({
                "responsavel_id":   equipe_ids.get(r_nome),
                "aprovador_id":     equipe_ids.get(a_nome) if a_nome != "—" else None,
                "consultado_id":    equipe_ids.get(c_nome) if c_nome != "—" else None,
                "informado_id":     equipe_ids.get(i_nome) if i_nome != "—" else None,
                "status":           novo_status,
                "impedimento":      novo_imp or None,
                "avanco_percent":   novo_avanco,
                "inicio_previsto":  novo_inicio.isoformat()  if novo_inicio  else None,
                "entrega_prevista": novo_entrega.isoformat() if novo_entrega else None,
                "entrega_real":     novo_real.isoformat()    if novo_real    else None,
                "gut_gravidade":    ng,"gut_urgencia":nu,"gut_tendencia":nt,
                "gut_score":        ng*nu*nt,
                "observacoes":      novo_obs or None,
            }).eq("id",t["id"]).execute()
            limpar_cache()
            st.success("✅ Salvo!"); st.rerun()

# ==========================================================
# PÁGINA: REUNIÃO 1:1
# ==========================================================
elif pagina_selecionada == "👤 Reunião 1:1":
    from datetime import date

    STATUS_OPCOES = ["A Iniciar","Em Andamento","Impedido","Concluído","N/A"]

    st.header("👤 Reunião 1:1")

    equipe_lista = carregar_equipe_ativa()
    if not equipe_lista:
        st.error("Nenhum colaborador ativo cadastrado."); st.stop()

    equipe_nomes = [e["nome"] for e in equipe_lista]
    equipe_ids   = {e["nome"]: e["id"] for e in equipe_lista}

    # ── SELEÇÃO DE COLABORADOR ─────────────────────────────
    c1, c2 = st.columns([4, 1])
    colab_nome = c1.selectbox("Colaborador", equipe_nomes,
                               key="sel_colab_1on1", label_visibility="collapsed")
    colab_id = equipe_ids[colab_nome]

    if c2.button("🔄 Atualizar", key="btn_refresh_1on1"):
        carregar_tarefas_colab.clear(); st.rerun()

    # ── CARREGA TAREFAS ────────────────────────────────────
    hoje    = date.today()
    tarefas = carregar_tarefas_colab(colab_id)

    # Anota papel e nome da obra em cada tarefa
    for t in tarefas:
        papeis = []
        if t.get("responsavel_id") == colab_id: papeis.append("R")
        if t.get("aprovador_id")   == colab_id: papeis.append("A")
        if t.get("consultado_id")  == colab_id: papeis.append("C")
        if t.get("informado_id")   == colab_id: papeis.append("I")
        t["_papel"]      = "/".join(papeis) or "?"
        t["_obra_nome"]  = (t.get("obras") or {}).get("nome") or "— Extra —"

    # ── MÉTRICAS ───────────────────────────────────────────
    hoje_iso    = hoje.isoformat()
    concluidas  = [t for t in tarefas if t["status"] == "Concluído"]
    otd_ok      = [t for t in concluidas
                   if t.get("entrega_real") and t.get("entrega_prevista")
                   and t["entrega_real"] <= t["entrega_prevista"]]
    otd_pct     = int(len(otd_ok) / len(concluidas) * 100) if concluidas else 0
    pendentes_n = sum(1 for t in tarefas if t["status"] not in ("Concluído","N/A"))
    atrasadas_n = sum(1 for t in tarefas
                      if t["status"] not in ("Concluído","N/A")
                      and t.get("entrega_prevista")
                      and t["entrega_prevista"] < hoje_iso)
    impedidas_n = sum(1 for t in tarefas if t["status"] == "Impedido")

    m1,m2,m3,m4,m5 = st.columns(5)
    m1.metric("📋 RACI total",  len(tarefas))
    m2.metric("⏳ Pendentes",   pendentes_n)
    m3.metric("🔴 Atrasadas",   atrasadas_n)
    m4.metric("🚧 Impedidas",   impedidas_n)
    m5.metric("📦 OTD",         f"{otd_pct}%",
              help="On-Time Delivery — tarefas concluídas dentro do prazo")

    st.divider()

    aba_tarefas, aba_extras, aba_email = st.tabs(
        ["📋 Tarefas RACI", "➕ Nova Extra", "📧 E-mail de Cobrança"])

    # ── ABA 1: TAREFAS RACI ────────────────────────────────
    with aba_tarefas:
        fc1, fc2 = st.columns([2, 2])
        f_papel  = fc1.selectbox("Papel",   ["Todos","R","A","C","I"],
                                  key="f_papel_1on1", label_visibility="collapsed")
        f_status = fc2.selectbox("Status",  ["Ativos"] + STATUS_OPCOES,
                                  key="f_status_1on1", label_visibility="collapsed")

        tf = list(tarefas)
        if f_papel  != "Todos":  tf = [t for t in tf if f_papel in t["_papel"]]
        if f_status == "Ativos": tf = [t for t in tf if t["status"] not in ("Concluído","N/A")]
        elif f_status != "Todos": tf = [t for t in tf if t["status"] == f_status]

        # Ordena: GUT desc, prazo asc
        tf = sorted(tf, key=lambda t: (-(t.get("gut_score") or 1),
                                        t.get("entrega_prevista") or "9999-99-99"))

        if not tf:
            st.info("Nenhuma tarefa para os filtros selecionados.")
        else:
            rows_1on1 = []
            for t in tf:
                farol = calcular_farol(t.get("entrega_prevista"), t["status"], hoje)
                rows_1on1.append({
                    "_id":       t["id"],
                    "Papel":     t["_papel"],
                    "GUT":       gut_emoji(t.get("gut_score") or 1),
                    "Obra":      t["_obra_nome"],
                    "Etapa":     t.get("etapa") or "—",
                    "Descrição": f"{farol} {t.get('descricao') or '—'}",
                    "Status":    t["status"],
                    "Desvio":    calcular_desvio(t.get("entrega_prevista"),
                                                  t.get("entrega_real"), t["status"], hoje),
                    "Av.%":      t.get("avanco_percent") or 0,
                })

            df_1on1 = pd.DataFrame(rows_1on1)
            st.caption(f"**{len(df_1on1)}** tarefa(s) · clique em uma linha para editar")

            sel_1on1 = st.dataframe(
                df_1on1.drop(columns=["_id"]),
                use_container_width=True,
                hide_index=True,
                height=min(420, 36 + 35 * len(df_1on1)),
                on_select="rerun",
                selection_mode="single-row",
                column_config={
                    "Papel":     st.column_config.TextColumn("Papel",    width="small"),
                    "GUT":       st.column_config.TextColumn("GUT",      width="small"),
                    "Obra":      st.column_config.TextColumn("Obra",     width="medium"),
                    "Etapa":     st.column_config.TextColumn("Etapa",    width="medium"),
                    "Descrição": st.column_config.TextColumn("Descrição",width="large"),
                    "Status":    st.column_config.TextColumn("Status",   width="medium"),
                    "Desvio":    st.column_config.TextColumn("Desvio",   width="small"),
                    "Av.%":      st.column_config.ProgressColumn("Av.%",
                                     min_value=0, max_value=100, width="small"),
                }
            )

            # ── FORMULÁRIO DE EDIÇÃO INLINE ───────────────────────
            linhas_sel = sel_1on1.selection.rows if sel_1on1.selection else []
            if linhas_sel:
                t = tf[linhas_sel[0]]
                st.divider()
                st.markdown(f"**✏️ {t['_obra_nome']}** · {t.get('etapa') or '—'} · {t.get('descricao','')}")

                ea, eb = st.columns(2)
                novo_status = ea.selectbox("Status", STATUS_OPCOES,
                    index=STATUS_OPCOES.index(t["status"]) if t["status"] in STATUS_OPCOES else 0,
                    key=f"1on1_st_{t['id']}")
                novo_avanco = eb.slider("% Avanço", 0, 100,
                    t.get("avanco_percent") or 0, step=5, key=f"1on1_av_{t['id']}")

                novo_imp = t.get("impedimento") or ""
                if novo_status == "Impedido":
                    novo_imp = st.text_input("🚧 Impedimento", value=novo_imp,
                                              key=f"1on1_imp_{t['id']}")

                fd1, fd2 = st.columns(2)
                novo_entrega = fd1.date_input("Entrega prevista",
                    value=parse_date(t.get("entrega_prevista")),
                    key=f"1on1_ent_{t['id']}", format="DD/MM/YYYY")
                novo_real = fd2.date_input("Entrega real",
                    value=parse_date(t.get("entrega_real")),
                    key=f"1on1_real_{t['id']}", format="DD/MM/YYYY")

                novo_obs = st.text_area("Obs.", value=t.get("observacoes") or "",
                    key=f"1on1_obs_{t['id']}", height=60)

                if st.button("💾 Salvar", type="primary", key=f"1on1_save_{t['id']}"):
                    supabase.table("obras_tarefas").update({
                        "status":           novo_status,
                        "impedimento":      novo_imp or None,
                        "avanco_percent":   novo_avanco,
                        "entrega_prevista": novo_entrega.isoformat() if novo_entrega else None,
                        "entrega_real":     novo_real.isoformat()    if novo_real    else None,
                        "observacoes":      novo_obs or None,
                    }).eq("id", t["id"]).execute()
                    limpar_cache(); st.success("✅ Salvo!"); st.rerun()

    # ── ABA 2: NOVA EXTRA ──────────────────────────────────
    with aba_extras:
        st.markdown(f"**Nova tarefa extra para {colab_nome}** _(obra_id = NULL)_")
        with st.form("nova_extra_1on1", clear_on_submit=True):
            ex1, ex2 = st.columns(2)
            ex_origem = ex1.text_input("Origem / Contexto",
                                        placeholder="Ex: Reunião semanal, Demanda direta")
            ex_status = ex2.selectbox("Status inicial", STATUS_OPCOES)
            ex_desc   = st.text_input("Descrição *")
            ex_obs    = st.text_area("Observação", height=60)

            ed1, ed2 = st.columns(2)
            ex_prazo   = ed1.date_input("Prazo previsto", value=None, format="DD/MM/YYYY")
            ex_entrega = ed2.date_input("Entrega real",   value=None, format="DD/MM/YYYY")

            eg1, eg2, eg3, eg4 = st.columns(4)
            ex_g = eg1.slider("G", 1, 5, 1, key="ex_g_1on1")
            ex_u = eg2.slider("U", 1, 5, 1, key="ex_u_1on1")
            ex_t = eg3.slider("T", 1, 5, 1, key="ex_t_1on1")
            eg4.metric("GUT", ex_g * ex_u * ex_t)

            if st.form_submit_button("💾 Criar Extra", type="primary"):
                if ex_desc:
                    supabase.table("obras_tarefas").insert({
                        "obra_id":          None,
                        "origem":           ex_origem or None,
                        "descricao":        ex_desc,
                        "observacoes":      ex_obs or None,
                        "responsavel_id":   colab_id,
                        "status":           ex_status,
                        "entrega_prevista": ex_prazo.isoformat()   if ex_prazo   else None,
                        "entrega_real":     ex_entrega.isoformat() if ex_entrega else None,
                        "gut_gravidade":    ex_g, "gut_urgencia": ex_u, "gut_tendencia": ex_t,
                        "gut_score":        ex_g * ex_u * ex_t,
                        "avanco_percent":   0,
                    }).execute()
                    limpar_cache(); st.success(f"✅ Extra criada para {colab_nome}!"); st.rerun()
                else:
                    st.warning("Descrição é obrigatória.")

    # ── ABA 3: E-MAIL DE COBRANÇA ──────────────────────────
    with aba_email:
        st.markdown(f"### 📧 Cobrança para {colab_nome}")

        pendentes_email = [t for t in tarefas if t["status"] not in ("Concluído","N/A")]
        pendentes_email = sorted(pendentes_email,
            key=lambda t: (-(t.get("gut_score") or 1),
                            t.get("entrega_prevista") or "9999-99-99"))

        if not pendentes_email:
            st.info("Nenhuma tarefa pendente — sem necessidade de cobrança!")
        else:
            atrasadas_e, urgentes_e, andamento_e = [], [], []
            for t in pendentes_email:
                farol = calcular_farol(t.get("entrega_prevista"), t["status"], hoje)
                linha = (t["_papel"], t["_obra_nome"],
                         t.get("descricao","—"),
                         t.get("entrega_prevista") or "sem prazo")
                if farol == "🔴":   atrasadas_e.append(linha)
                elif farol == "🟡": urgentes_e.append(linha)
                else:               andamento_e.append(linha)

            linhas = [f"Olá, {colab_nome}!\n",
                      "Segue o resumo das suas tarefas em aberto:\n"]

            if atrasadas_e:
                linhas.append("🔴 ATRASADAS:")
                for papel, obra, desc, prazo in atrasadas_e:
                    linhas.append(f"  • [{papel}] {obra} — {desc}  (prazo: {prazo})")

            if urgentes_e:
                linhas.append("\n🟡 VENCENDO EM BREVE:")
                for papel, obra, desc, prazo in urgentes_e:
                    linhas.append(f"  • [{papel}] {obra} — {desc}  (prazo: {prazo})")

            if andamento_e:
                linhas.append("\n🟢 EM ANDAMENTO:")
                for papel, obra, desc, prazo in andamento_e:
                    linhas.append(f"  • [{papel}] {obra} — {desc}  (prazo: {prazo})")

            linhas.append(f"\nTotal: {len(pendentes_email)} tarefa(s) pendente(s)")
            linhas.append(f"OTD histórico: {otd_pct}% ({len(otd_ok)}/{len(concluidas)} no prazo)")
            linhas.append("\nQualquer dúvida, estou à disposição!")

            st.text_area("Corpo do e-mail", value="\n".join(linhas),
                          height=380, key="email_1on1")
            st.caption("Copie o texto acima e cole no seu cliente de e-mail.")

# ==========================================================
# PÁGINA: FINANCEIRO — DASHBOARD DE CUSTOS
# ==========================================================
elif pagina_selecionada == "💰 Financeiro":
    import plotly.graph_objects as go
    import plotly.express as px

    # ── FUNÇÕES DE CARGA ────────────────────────────────────

    @st.cache_data(ttl=300)
    def carregar_obras_financeiro():
        resp = supabase.table("obras_financeiro")\
            .select("*, obras(nome, status, modalidade, cliente)")\
            .execute()
        if not resp.data:
            return pd.DataFrame()
        df = pd.DataFrame(resp.data)
        df["obra_nome"]   = df["obras"].apply(lambda x: (x or {}).get("nome"))
        df["obra_status"] = df["obras"].apply(lambda x: (x or {}).get("status"))
        df["modalidade"]  = df["obras"].apply(lambda x: (x or {}).get("modalidade"))
        df["cliente"]     = df["obras"].apply(lambda x: (x or {}).get("cliente"))
        df = df.drop(columns=["obras"])
        for c in [
            "volume", "volume_projeto", "faturamento_total", "faturamento_civil",
            "faturamento_direto", "custo_total", "despesas_indiretas", "impostos",
            "lucro", "concreto", "aco_estrutural", "formas", "mo_producao",
            "eps", "estuque", "projetos", "descida_agua", "insertos", "consoles",
            "investimentos", "neoprene", "materiais_consumo", "equip_fab",
            "custos_indiretos", "pecas_consorcio", "frete", "equip_montagem",
            "mo_montagem", "despesas_equipe", "topografia", "mobilizacao",
            "equip_aux_montagem", "outros", "eventuais", "despesas_comerciais",
            "cimento_cliente_ton", "cimento_civil_ton",
        ]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    @st.cache_data(ttl=300)
    def carregar_medicoes():
        rows, page, size = [], 0, 1000
        q = (supabase.table("notas_fiscais")
             .select("obra_id, data_acao, descricao, tipo, valor")
             .not_.in_("tipo", ["NOTA FISCAL CANCELADA", "NOTA FISCAL REMESSA"]))
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty: return df
        df["valor"]     = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
        df["data_acao"] = pd.to_datetime(df["data_acao"], errors="coerce")
        df["mes"]       = df["data_acao"].dt.to_period("M").astype(str)
        return df

    @st.cache_data(ttl=300)
    def carregar_medicoes_completas(obra_id):
        rows, page, size = [], 0, 1000
        q = (supabase.table("notas_fiscais")
             .select("*")
             .eq("obra_id", obra_id)
             .not_.in_("tipo", ["NOTA FISCAL CANCELADA"])
             .order("data_acao", desc=True))
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty: return df
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
        return df

    @st.cache_data(ttl=300)
    def volume_fabricado_por_obra():
        rows, page, size = [], 0, 1000
        q = supabase.table("producao_fabricacao").select("obra_id, volume_teorico")
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        if not rows: return {}
        df = pd.DataFrame(rows)
        df["volume_teorico"] = pd.to_numeric(df["volume_teorico"], errors="coerce").fillna(0)
        return df.groupby("obra_id")["volume_teorico"].sum().to_dict()

    # ── FUNÇÕES DE CÁLCULO ────────────────────────────────────

    def calcular_custos_categorias(row):
        def v(col): return float(row.get(col) or 0) if pd.notna(row.get(col)) else 0.0
        return {
            "custo_fabricacao": sum(v(c) for c in [
                "concreto", "aco_estrutural", "formas", "mo_producao",
                "materiais_consumo", "equip_fab", "custos_indiretos",
                "eps", "estuque", "insertos", "consoles", "neoprene",
                "descida_agua", "pecas_consorcio", "investimentos"]),
            "custo_transporte": v("frete"),
            "custo_montagem":   sum(v(c) for c in [
                "mo_montagem", "equip_montagem", "equip_aux_montagem",
                "despesas_equipe", "topografia", "mobilizacao"]),
        }

    def vol_ref_row(row):
        """Retorna (volume, origem) de uma linha do DataFrame obras_financeiro."""
        vp = row.get("volume_projeto")
        vc = row.get("volume")
        if vp is not None and not pd.isna(vp) and float(vp) > 0:
            return float(vp), "projeto"
        if vc is not None and not pd.isna(vc) and float(vc) > 0:
            return float(vc), "comercial"
        return None, None

    def fmt_brl_fin(v):
        if v is None or (isinstance(v, float) and pd.isna(v)) or v == 0:
            return "R$ 0"
        sinal = "-" if v < 0 else ""
        return (f"{sinal}R$ "
                + f"{abs(v):,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))

    def fmt_milhao(v):
        if v is None or (isinstance(v, float) and pd.isna(v)) or v == 0:
            return "R$ 0"
        av, sinal = abs(v), ("-" if v < 0 else "")
        if av >= 1_000_000:
            return f"{sinal}R$ {av/1_000_000:.1f}M".replace(".", ",")
        if av >= 1_000:
            return f"{sinal}R$ {av/1_000:.0f}k".replace(".", ",")
        return fmt_brl_fin(v)

    # ── CARREGA DADOS ─────────────────────────────────────────
    st.header("💰 Financeiro")

    with st.spinner("Carregando dados financeiros..."):
        df_of  = carregar_obras_financeiro()
        df_med = carregar_medicoes()

    if df_of.empty:
        st.warning("Nenhum dado em obras_financeiro. "
                   "Importe a rota 12 na seção Importador.")
        st.stop()

    hdr1, hdr2 = st.columns([6, 1])
    if hdr2.button("🔄 Atualizar", key="btn_fin_refresh"):
        carregar_obras_financeiro.clear()
        carregar_medicoes.clear()
        carregar_medicoes_completas.clear()
        volume_fabricado_por_obra.clear()
        st.rerun()

    tab_cart, tab_obra = st.tabs(["📊 Carteira", "🏗️ Obra"])

    # ══════════════════════════════════════════════════════════
    # ABA 1 — CARTEIRA
    # ══════════════════════════════════════════════════════════
    with tab_cart:
        fc1, fc2, _ = st.columns([2, 2, 3])
        sel_periodo_c = fc1.selectbox(
            "Período", ["Ativas", "Histórico", "Todas"], key="cart_periodo")
        mods_disp = ["Todas"] + sorted(
            m for m in df_of["modalidade"].dropna().unique() if m)
        sel_modal = fc2.selectbox("Modalidade", mods_disp, key="cart_modal")

        df_cart = df_of.copy()
        if sel_periodo_c == "Ativas":
            df_cart = df_cart[df_cart["obra_status"] == "Em Andamento"]
        elif sel_periodo_c == "Histórico":
            df_cart = df_cart[df_cart["obra_status"].isin(["Concluída", "Cancelada"])]
        if sel_modal != "Todas":
            df_cart = df_cart[df_cart["modalidade"] == sel_modal]

        if df_cart.empty:
            st.info("Nenhuma obra para o filtro selecionado.")
        else:
            ids_cart   = set(df_cart["obra_id"].dropna().astype(int))
            df_med_c   = (df_med[df_med["obra_id"].isin(ids_cart)].copy()
                          if not df_med.empty else pd.DataFrame())
            fat_total  = df_cart["faturamento_total"].fillna(0).sum()
            faturado_c = df_med_c["valor"].sum() if not df_med_c.empty else 0.0
            saldo_c    = fat_total - faturado_c
            pct_c      = faturado_c / fat_total if fat_total > 0 else 0.0

            # Métricas
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("💼 Carteira total", fmt_milhao(fat_total))
            m2.metric("✅ Faturado",        fmt_milhao(faturado_c))
            m3.metric("⏳ Saldo",           fmt_milhao(saldo_c))
            m4.metric("🏗️ Obras",           str(len(df_cart)))
            st.progress(min(pct_c, 1.0))
            st.caption(f"{pct_c*100:.1f}% faturado da carteira total")

            st.divider()

            # Situação por obra — barras horizontais empilhadas
            st.markdown("#### 📊 Situação por obra")
            med_por_obra = (df_med_c.groupby("obra_id")["valor"].sum()
                            if not df_med_c.empty else pd.Series(dtype=float))

            rows_bar = []
            for _, r in df_cart.iterrows():
                oid  = r.get("obra_id")
                fat  = float(r.get("faturamento_total") or 0)
                fatd = float(med_por_obra.get(oid, 0))
                pct  = fatd / fat * 100 if fat > 0 else 0.0
                rows_bar.append({
                    "obra":    r.get("obra_nome") or f"ID {oid}",
                    "faturado": fatd,
                    "saldo":    max(0.0, fat - fatd),
                    "fat_total": fat,
                    "pct":      pct,
                })
            df_bar = (pd.DataFrame(rows_bar)
                      .sort_values("saldo", ascending=False)
                      .reset_index(drop=True))

            def _cor(pct):
                return "#10b981" if pct >= 80 else ("#f59e0b" if pct >= 50 else "#ef4444")

            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                y=df_bar["obra"], x=df_bar["faturado"],
                orientation="h", name="Faturado",
                marker_color=[_cor(p) for p in df_bar["pct"]],
                customdata=df_bar[["saldo", "pct", "fat_total"]].values,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Faturado: R$ %{x:,.0f}<br>"
                    "Saldo: R$ %{customdata[0]:,.0f}<br>"
                    "%{customdata[1]:.1f}% de R$ %{customdata[2]:,.0f}"
                    "<extra></extra>")))
            fig_bar.add_trace(go.Bar(
                y=df_bar["obra"], x=df_bar["saldo"],
                orientation="h", name="Saldo",
                marker_color="rgba(200,200,200,0.35)",
                hoverinfo="skip"))
            fig_bar.update_layout(
                barmode="stack",
                xaxis=dict(title="R$", tickformat=",.0f"),
                yaxis=dict(autorange="reversed"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=40, b=20, l=10, r=10),
                height=min(600, 40 * len(df_bar) + 80))
            st.plotly_chart(fig_bar, use_container_width=True, key="chart_cart_bar")
            st.caption("🟢 ≥ 80% faturado   🟡 50–80%   🔴 < 50%")

            st.divider()

            # Evolução mensal do faturamento (barras + acumulado)
            st.markdown("#### 📅 Evolução mensal do faturamento")
            if not df_med_c.empty:
                ev_men = (df_med_c.groupby("mes")["valor"].sum()
                          .reset_index().sort_values("mes"))
                ev_men["acumulado"] = ev_men["valor"].cumsum()
                fig_ev_c = go.Figure()
                fig_ev_c.add_trace(go.Bar(
                    x=ev_men["mes"], y=ev_men["valor"],
                    name="Mensal", marker_color="#1976D2",
                    hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"))
                fig_ev_c.add_trace(go.Scatter(
                    x=ev_men["mes"], y=ev_men["acumulado"],
                    name="Acumulado", yaxis="y2",
                    mode="lines+markers",
                    line=dict(color="#E53935", width=2),
                    hovertemplate="Acum.: R$ %{y:,.0f}<extra></extra>"))
                fig_ev_c.update_layout(
                    yaxis=dict(title="R$ Mensal", tickformat=",.0f"),
                    yaxis2=dict(title="Acumulado", overlaying="y", side="right",
                                tickformat=",.0f", showgrid=False),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                    margin=dict(t=50, b=20), height=320)
                st.plotly_chart(fig_ev_c, use_container_width=True, key="chart_cart_ev")
            else:
                st.info("Sem medições para o período filtrado.")

            st.divider()

            # Distribuição por modalidade
            st.markdown("#### 🥧 Distribuição por modalidade")
            df_mod_p = df_cart[df_cart["modalidade"].notna()].copy()
            if df_mod_p.empty:
                st.info("Modalidade não definida nas obras.")
            else:
                mod_grp = (df_mod_p.groupby("modalidade")["faturamento_total"]
                           .sum().fillna(0).reset_index())
                mod_grp = mod_grp[mod_grp["faturamento_total"] > 0]
                if mod_grp.empty:
                    st.info("Modalidade não definida nas obras.")
                else:
                    fig_mod = px.pie(
                        mod_grp, names="modalidade", values="faturamento_total",
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Set2)
                    fig_mod.update_traces(
                        textinfo="label+percent",
                        hovertemplate="%{label}<br>R$ %{value:,.0f}<br>%{percent}<extra></extra>")
                    fig_mod.update_layout(
                        showlegend=False,
                        margin=dict(t=20, b=20, l=20, r=20), height=320)
                    st.plotly_chart(fig_mod, use_container_width=True, key="chart_modal")

    # ══════════════════════════════════════════════════════════
    # ABA 2 — OBRA
    # ══════════════════════════════════════════════════════════
    with tab_obra:
        ob1, ob2 = st.columns(2)
        sel_ob_per = ob1.selectbox("Período", ["Ativas", "Todas"], key="obra_periodo")

        df_ob_disp = df_of.copy()
        if sel_ob_per == "Ativas":
            df_ob_disp = df_ob_disp[df_ob_disp["obra_status"] == "Em Andamento"]
        df_ob_disp = df_ob_disp.sort_values("obra_nome")

        if df_ob_disp.empty:
            st.info("Nenhuma obra disponível.")
            st.stop()

        obras_map_fin = {}
        for _, r in df_ob_disp.iterrows():
            cod   = str(r.get("codigo_produto") or "").strip()
            nome  = str(r.get("obra_nome") or "?")
            label = f"{cod} — {nome}" if cod else nome
            obras_map_fin[label] = r

        sel_obra_label = ob2.selectbox("Obra", list(obras_map_fin.keys()),
                                        key="sel_obra_fin")
        row_obra  = obras_map_fin[sel_obra_label]
        obra_id_f = row_obra.get("obra_id")

        fat_total_o = float(row_obra.get("faturamento_total") or 0)
        lucro_o     = row_obra.get("lucro")

        df_med_o  = (df_med[df_med["obra_id"] == obra_id_f].copy()
                     if not df_med.empty else pd.DataFrame())
        faturado_o = float(df_med_o["valor"].sum()) if not df_med_o.empty else 0.0
        saldo_o    = fat_total_o - faturado_o
        pct_fin_o  = faturado_o / fat_total_o if fat_total_o > 0 else 0.0

        # Painel financeiro
        pa1, pa2, pa3, pa4 = st.columns(4)
        pa1.metric("📋 Contratado",     fmt_brl_fin(fat_total_o))
        pa2.metric("✅ Faturado",        fmt_brl_fin(faturado_o))
        pa3.metric("⏳ Saldo",           fmt_brl_fin(saldo_o))
        try:
            lucro_val = float(lucro_o) if lucro_o is not None else None
            lucro_str = fmt_brl_fin(lucro_val) if lucro_val is not None and not pd.isna(lucro_val) else "—"
        except Exception:
            lucro_str = "—"
        pa4.metric("📈 Margem prevista", lucro_str)
        st.progress(min(pct_fin_o, 1.0))
        st.caption(f"{pct_fin_o*100:.1f}% faturado")

        st.divider()

        # Avanço físico vs financeiro
        st.markdown("#### 📐 Avanço físico vs financeiro")
        vref, vref_origem = vol_ref_row(row_obra)
        vol_fab_map = volume_fabricado_por_obra()
        vol_fab_o   = float(vol_fab_map.get(obra_id_f, 0))

        av1, av2 = st.columns(2)
        with av1:
            st.markdown("**Físico (fabricação)**")
            if vref and vref > 0:
                pct_fis = min(vol_fab_o / vref, 1.0)
                st.progress(pct_fis)
                st.caption(
                    f"{vol_fab_o:,.1f} m³ fab. / {vref:,.1f} m³ ref. ({vref_origem})"
                    f" — {pct_fis*100:.1f}%".replace(",", "."))
            else:
                pct_fis = 0.0
                st.info("Volume de referência não disponível para esta obra.")

        with av2:
            st.markdown("**Financeiro (faturamento)**")
            st.progress(min(pct_fin_o, 1.0))
            st.caption(
                f"{fmt_brl_fin(faturado_o)} / {fmt_brl_fin(fat_total_o)}"
                f" — {pct_fin_o*100:.1f}%")

        # Gap físico vs financeiro
        if vref and vref > 0 and fat_total_o > 0:
            diff = pct_fis * 100 - pct_fin_o * 100
            valor_a_fat = (pct_fis - pct_fin_o) * fat_total_o
            if abs(diff) < 5:
                st.success(f"✅ Avanços alinhados ({diff:+.1f}%)")
            elif diff > 0:
                st.info(
                    f"📐 Físico à frente do financeiro em {diff:.1f}%"
                    f" — {fmt_brl_fin(valor_a_fat)} a faturar")
            else:
                st.warning(f"💰 Financeiro à frente do físico em {abs(diff):.1f}%")

        st.divider()

        # Evolução mensal da obra por descrição
        st.markdown("#### 📅 Faturamento mensal por tipo")
        if not df_med_o.empty and "mes" in df_med_o.columns:
            ev_obra = (df_med_o.groupby(["mes", "descricao"])["valor"]
                       .sum().reset_index())
            fig_ev_o = px.bar(
                ev_obra, x="mes", y="valor", color="descricao",
                barmode="stack",
                labels={"mes": "", "valor": "R$", "descricao": "Tipo"},
                color_discrete_sequence=px.colors.qualitative.Set2)
            fig_ev_o.update_layout(
                yaxis_tickformat=",.0f", yaxis_tickprefix="R$ ",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=50, b=20), height=280)
            fig_ev_o.update_traces(
                hovertemplate="%{x}<br>%{fullData.name}<br>R$ %{y:,.0f}<extra></extra>")
            st.plotly_chart(fig_ev_o, use_container_width=True, key="chart_ev_obra")
        else:
            st.info("Sem medições para esta obra.")

        st.divider()

        # Breakdown de custo previsto
        st.markdown("#### 💰 Breakdown de custo previsto")
        custos_cat = calcular_custos_categorias(row_obra)
        cf = custos_cat["custo_fabricacao"]
        ct = custos_cat["custo_transporte"]
        cm = custos_cat["custo_montagem"]
        custo_total_cat = cf + ct + cm

        bc1, bc2 = st.columns(2)
        with bc1:
            if custo_total_cat > 0:
                _nomes  = ["Fabricação", "Transporte", "Montagem"]
                _vals   = [cf, ct, cm]
                _cores  = ["#1976D2", "#FB8C00", "#43A047"]
                _filt_n = [n for n, v in zip(_nomes, _vals) if v > 0]
                _filt_v = [v for v in _vals if v > 0]
                _filt_c = [c for c, v in zip(_cores, _vals) if v > 0]
                fig_bc  = px.pie(
                    names=_filt_n, values=_filt_v,
                    hole=0.4, color_discrete_sequence=_filt_c)
                fig_bc.update_traces(
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>R$ %{value:,.0f}<br>%{percent}<extra></extra>")
                fig_bc.update_layout(
                    showlegend=False,
                    margin=dict(t=20, b=20, l=10, r=10), height=300)
                st.plotly_chart(fig_bc, use_container_width=True, key="chart_bc_pie")
            else:
                st.info("Breakdown não disponível para esta obra.")

        with bc2:
            if custo_total_cat > 0:
                for nome_cat, val_cat in [("Fabricação", cf),
                                           ("Transporte", ct),
                                           ("Montagem",   cm)]:
                    pct_cat = val_cat / custo_total_cat * 100 if custo_total_cat else 0
                    st.metric(nome_cat, fmt_brl_fin(val_cat),
                              delta=f"{pct_cat:.1f}% do custo total",
                              delta_color="off")

        st.divider()

        # Tabela de NFs
        st.markdown("#### 📄 Notas fiscais / medições")
        with st.spinner("Carregando NFs..."):
            df_nfs = carregar_medicoes_completas(obra_id_f)

        if df_nfs.empty:
            st.info("Nenhuma NF para esta obra.")
        else:
            cols_nf = [c for c in ["data_acao", "numero_nf", "descricao", "tipo", "valor"]
                       if c in df_nfs.columns]
            df_nf_show = df_nfs[cols_nf].copy()
            df_nf_show["valor_fmt"] = df_nf_show["valor"].apply(fmt_brl_fin)
            st.dataframe(
                df_nf_show.drop(columns=["valor"]).rename(columns={
                    "data_acao":  "Data",
                    "numero_nf":  "NF",
                    "descricao":  "Descrição",
                    "tipo":       "Tipo",
                    "valor_fmt":  "Valor",
                }),
                use_container_width=True, hide_index=True,
                height=min(500, 36 + 35 * len(df_nf_show)))
            st.caption(f"Total: {fmt_brl_fin(df_nf_show['valor'].sum())}")

# ==========================================================
# PÁGINA: PRODUÇÃO — DASHBOARD OPERACIONAL
# ==========================================================
elif pagina_selecionada == "🏭 Produção":
    import plotly.graph_objects as go
    import plotly.express as px
    from datetime import date, timedelta

    # ── FUNÇÕES DE CARGA ────────────────────────────────────
    @st.cache_data(ttl=600)
    def carregar_fabricacao(inicio, fim):
        def _query(cols):
            rows, page, size = [], 0, 1000
            q = supabase.table("producao_fabricacao").select(cols)
            if inicio: q = q.gte("data_fabricacao", inicio)
            if fim:    q = q.lte("data_fabricacao", fim)
            while True:
                resp = q.range(page * size, (page + 1) * size - 1).execute()
                rows.extend(resp.data)
                if len(resp.data) < size: break
                page += 1
            return rows

        try:
            rows = _query("obra_id, produto, etapa, volume_total, peso_aco,"
                          " peso_aco_frouxo, peso_aco_protendido, data_fabricacao")
        except Exception:
            rows = _query("obra_id, produto, etapa, volume_total, peso_aco, data_fabricacao")

        df = pd.DataFrame(rows)
        if df.empty: return df
        for col in ["volume_total", "peso_aco", "peso_aco_frouxo", "peso_aco_protendido"]:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df["mes"] = pd.to_datetime(df["data_fabricacao"], errors="coerce").dt.to_period("M").astype(str)

        # Classifica produto → tipo para split da cordoalha
        def _tipo_produto(nome):
            n = str(nome).upper()
            if "LAJE" in n:  return "Laje"
            if "VIGA" in n:  return "Viga"
            return "Outros"

        df["tipo_produto"] = df["produto"].apply(_tipo_produto)
        # Cordoalha por tipo: distribui peso_aco_protendido conforme o tipo
        df["cord_viga"]  = df["peso_aco_protendido"].where(df["tipo_produto"] == "Viga",  0)
        df["cord_laje"]  = df["peso_aco_protendido"].where(df["tipo_produto"] == "Laje",  0)
        df["cord_outros"] = df["peso_aco_protendido"].where(df["tipo_produto"] == "Outros", 0)
        return df

    @st.cache_data(ttl=600)
    def carregar_transporte_prod(inicio, fim):
        rows, page, size = [], 0, 1000
        q = (supabase.table("producao_transporte")
             .select("obra_id, produto, etapa, volume_real, data_expedicao"))
        if inicio: q = q.gte("data_expedicao", inicio)
        if fim:    q = q.lte("data_expedicao", fim)
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty: return df
        df["volume_real"] = pd.to_numeric(df["volume_real"], errors="coerce").fillna(0)
        df["mes"] = pd.to_datetime(df["data_expedicao"], errors="coerce").dt.to_period("M").astype(str)
        return df

    @st.cache_data(ttl=600)
    def carregar_montagem_prod(inicio, fim):
        rows, page, size = [], 0, 1000
        q = (supabase.table("producao_montagem")
             .select("obra_id, produto, etapa, volume_total, data_montagem"))
        if inicio: q = q.gte("data_montagem", inicio)
        if fim:    q = q.lte("data_montagem", fim)
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty: return df
        df["volume_total"] = pd.to_numeric(df["volume_total"], errors="coerce").fillna(0)
        df["mes"] = pd.to_datetime(df["data_montagem"], errors="coerce").dt.to_period("M").astype(str)
        return df

    @st.cache_data(ttl=600)
    def carregar_patio(inicio, fim):
        """Carrega peca+obra+datas para calcular prazo de pátio (fab → expedição)."""
        rows_f, page, size = [], 0, 1000
        qf = supabase.table("producao_fabricacao").select("obra_id, peca, data_fabricacao")
        if inicio: qf = qf.gte("data_fabricacao", inicio)
        if fim:    qf = qf.lte("data_fabricacao", fim)
        while True:
            resp = qf.range(page * size, (page + 1) * size - 1).execute()
            rows_f.extend(resp.data)
            if len(resp.data) < size: break
            page += 1

        rows_t, page = [], 0
        qt = supabase.table("producao_transporte").select("obra_id, peca, data_expedicao")
        if inicio: qt = qt.gte("data_expedicao", inicio)
        while True:
            resp = qt.range(page * size, (page + 1) * size - 1).execute()
            rows_t.extend(resp.data)
            if len(resp.data) < size: break
            page += 1

        df_f = pd.DataFrame(rows_f)
        df_t = pd.DataFrame(rows_t)
        if df_f.empty or df_t.empty:
            return pd.DataFrame()

        df_f["data_fabricacao"] = pd.to_datetime(df_f["data_fabricacao"], errors="coerce")
        df_t["data_expedicao"]  = pd.to_datetime(df_t["data_expedicao"],  errors="coerce")
        fab_min = df_f.groupby(["obra_id", "peca"])["data_fabricacao"].min().reset_index()
        exp_min = df_t.groupby(["obra_id", "peca"])["data_expedicao"].min().reset_index()
        merged  = fab_min.merge(exp_min, on=["obra_id", "peca"], how="inner")
        merged["dias_patio"] = (merged["data_expedicao"] - merged["data_fabricacao"]).dt.days
        return merged[merged["dias_patio"] >= 0].copy()

    @st.cache_data(ttl=600)
    def carregar_custos_prod():
        """Carrega custos com conta_gerencial para análise de eficiência R$/m³."""
        rows, page, size = [], 0, 1000
        while True:
            resp = (supabase.table("custos")
                    .select("data, centro_custos, conta_gerencial, conta_macro, valor_global")
                    .range(page * size, (page + 1) * size - 1)
                    .execute())
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty: return df
        df["valor_global"] = pd.to_numeric(df["valor_global"], errors="coerce").fillna(0)
        df["data_dt"]  = pd.to_datetime(df["data"], errors="coerce")
        df["mes"]      = df["data_dt"].dt.to_period("M").astype(str)
        df["ano"]      = df["data_dt"].dt.year.astype(str)
        df["mes_num"]  = df["data_dt"].dt.month
        return df

    def fmt_m3(v):
        if pd.isna(v) or v == 0: return "0 m³"
        return f"{v:,.1f} m³".replace(",", "X").replace(".", ",").replace("X", ".")

    # ── HEADER + FILTROS GLOBAIS ────────────────────────────
    st.header("🏭 Dashboard de Produção")

    obras_lista = carregar_obras_ativas()
    obras_map   = {o["id"]: f"{o['codigo']} — {o['nome']}" for o in obras_lista}

    hoje = date.today()

    f1, f2, f3, f4 = st.columns([2, 2, 4, 1])
    tipo_periodo = f1.radio("Tipo de período", ["Predefinido", "Personalizado"],
                             horizontal=True, key="prod_tipo_periodo")

    if tipo_periodo == "Predefinido":
        periodo_opcoes = {
            "Hoje":             0,
            "Últimos 7 dias":   0.25,
            "Últimos 30 dias":  1,
            "Últimos 3 meses":  3,
            "Últimos 6 meses":  6,
            "Últimos 12 meses": 12,
            "Últimos 24 meses": 24,
            "Todo o período":   None,
        }
        sel_periodo = f2.selectbox("Período", list(periodo_opcoes.keys()),
                                    index=5, key="prod_periodo")
        n_meses = periodo_opcoes[sel_periodo]
        if n_meses is None:
            inicio_str, fim_str = None, hoje.isoformat()
        elif n_meses == 0:
            inicio_str = hoje.isoformat()
            fim_str    = hoje.isoformat()
        else:
            inicio_str = (hoje - timedelta(days=int(n_meses * 30.5))).strftime("%Y-%m-%d")
            fim_str    = hoje.isoformat()
        n_meses_ev = int(n_meses or 24)
    else:
        intervalo = f2.date_input(
            "Intervalo", value=(hoje - timedelta(days=365), hoje),
            key="prod_intervalo")
        if isinstance(intervalo, (list, tuple)) and len(intervalo) == 2:
            inicio_str = intervalo[0].isoformat()
            fim_str    = intervalo[1].isoformat()
            n_meses_ev = max(1, int((intervalo[1] - intervalo[0]).days / 30.5))
        else:
            inicio_str = hoje.isoformat()
            fim_str    = hoje.isoformat()
            n_meses_ev = 1

    sel_obras = f3.multiselect("Obras", list(obras_map.values()), key="prod_obras",
                                placeholder="Todas as obras")
    if f4.button("🔄 Atualizar", key="btn_prod_refresh"):
        carregar_fabricacao.clear()
        carregar_transporte_prod.clear()
        carregar_montagem_prod.clear()
        carregar_patio.clear()
        carregar_custos_prod.clear()
        st.rerun()

    with st.spinner("Carregando dados de produção..."):
        df_fab = carregar_fabricacao(inicio_str, fim_str)
        df_tra = carregar_transporte_prod(inicio_str, fim_str)
        df_mon = carregar_montagem_prod(inicio_str, fim_str)

    if df_fab.empty and df_tra.empty and df_mon.empty:
        st.warning("Nenhum dado de produção para o período selecionado.")
        st.stop()

    obra_ids_sel = {oid for oid, nome in obras_map.items() if nome in sel_obras}
    def filtrar_obs(df):
        return df[df["obra_id"].isin(obra_ids_sel)] if (obra_ids_sel and not df.empty) else df

    df_fab_f = filtrar_obs(df_fab)
    df_tra_f = filtrar_obs(df_tra)
    df_mon_f = filtrar_obs(df_mon)

    # ── ABAS ────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["🏭 Produção", "⚡ Eficiência (R$/m³)", "🔩 Insumos"])

    # ════════════════════════════════════════════════════════
    # ABA 1 — PRODUÇÃO
    # ════════════════════════════════════════════════════════
    with tab1:
        vol_fab = df_fab_f["volume_total"].sum() if not df_fab_f.empty else 0
        vol_exp = df_tra_f["volume_real"].sum()  if not df_tra_f.empty else 0
        vol_mon = df_mon_f["volume_total"].sum() if not df_mon_f.empty else 0
        gap_pat = max(0.0, vol_fab - vol_exp)
        gap_can = max(0.0, vol_exp - vol_mon)

        # ── Bloco A — Funil de fluxo físico ────────────────
        st.markdown("#### 🔄 Funil de fluxo físico")

        def _cor_gap(pct_gap):
            """pct_gap = gap / volume_referência * 100"""
            if pct_gap >= 30: return "#E53935"
            if pct_gap >= 15: return "#FB8C00"
            return "#43A047"

        pct_gap_pat = (gap_pat / vol_fab * 100) if vol_fab else 0
        pct_gap_can = (gap_can / vol_exp * 100) if vol_exp else 0
        pct_exp     = (vol_exp / vol_fab * 100)  if vol_fab else 0
        pct_mon     = (vol_mon / vol_exp * 100)  if vol_exp else 0

        cor_gap_pat = _cor_gap(pct_gap_pat)
        cor_gap_can = _cor_gap(pct_gap_can)

        fu1, fu_a1, fu2, fu_a2, fu3 = st.columns([3, 1, 3, 1, 3])
        with fu1:
            st.markdown(
                f"""<div style="background:#1976D2;border-radius:10px;padding:18px 10px;
                text-align:center;color:white">
                <div style="font-size:1.7rem;font-weight:700">{fmt_m3(vol_fab)}</div>
                <div style="font-size:0.95rem;opacity:.85">🏭 FABRICADO</div>
                <div style="font-size:0.8rem;opacity:.7;margin-top:4px">100%</div>
                </div>""", unsafe_allow_html=True)
        with fu_a1:
            st.markdown(
                f"""<div style="text-align:center;padding-top:22px">
                <div style="font-size:1.4rem">→</div>
                <div style="font-size:0.75rem;color:{cor_gap_pat};font-weight:600;margin-top:4px">
                ▼ {fmt_m3(gap_pat)}<br>no pátio</div>
                </div>""", unsafe_allow_html=True)
        with fu2:
            st.markdown(
                f"""<div style="background:#43A047;border-radius:10px;padding:18px 10px;
                text-align:center;color:white">
                <div style="font-size:1.7rem;font-weight:700">{fmt_m3(vol_exp)}</div>
                <div style="font-size:0.95rem;opacity:.85">🚛 EXPEDIDO</div>
                <div style="font-size:0.8rem;opacity:.7;margin-top:4px">{pct_exp:.1f}% do fab.</div>
                </div>""", unsafe_allow_html=True)
        with fu_a2:
            st.markdown(
                f"""<div style="text-align:center;padding-top:22px">
                <div style="font-size:1.4rem">→</div>
                <div style="font-size:0.75rem;color:{cor_gap_can};font-weight:600;margin-top:4px">
                ▼ {fmt_m3(gap_can)}<br>no canteiro</div>
                </div>""", unsafe_allow_html=True)
        with fu3:
            st.markdown(
                f"""<div style="background:#E53935;border-radius:10px;padding:18px 10px;
                text-align:center;color:white">
                <div style="font-size:1.7rem;font-weight:700">{fmt_m3(vol_mon)}</div>
                <div style="font-size:0.95rem;opacity:.85">🏗️ MONTADO</div>
                <div style="font-size:0.8rem;opacity:.7;margin-top:4px">{pct_mon:.1f}% do exp.</div>
                </div>""", unsafe_allow_html=True)

        st.divider()

        # ── Bloco B — Evolução mensal (áreas sobrepostas) ──
        st.markdown("#### 📈 Evolução mensal (m³)")
        ev1, ev2 = st.columns([5, 1])
        ev_n = ev2.number_input("Meses", 1, 84, min(n_meses_ev, 84),
                                 key="ev_prod_n", step=1)

        def agg_mes(df, col_vol, label):
            if df.empty:
                return pd.DataFrame({"mes": pd.Series(dtype=str),
                                     label: pd.Series(dtype=float)})
            g = df.groupby("mes")[col_vol].sum().reset_index()
            g.columns = ["mes", label]
            return g

        df_ev_fab = agg_mes(df_fab_f, "volume_total", "Fabricação")
        df_ev_tra = agg_mes(df_tra_f, "volume_real",  "Expedição")
        df_ev_mon = agg_mes(df_mon_f, "volume_total", "Montagem")

        todos_meses = sorted(set(
            list(df_ev_fab["mes"]) + list(df_ev_tra["mes"]) + list(df_ev_mon["mes"])))
        todos_meses = [m for m in todos_meses if m and m not in ("NaT", "nan")]
        todos_meses = todos_meses[-int(ev_n):]

        df_ev = pd.DataFrame({"mes": todos_meses})
        for sub in [df_ev_fab, df_ev_tra, df_ev_mon]:
            df_ev = df_ev.merge(sub, on="mes", how="left")
        df_ev = df_ev.fillna(0)

        fig_ev = go.Figure()
        series_ev = [
            ("Fabricação", "#1976D2", "rgba(25,118,210,0.15)"),
            ("Expedição",  "#43A047", "rgba(67,160,71,0.15)"),
            ("Montagem",   "#E53935", "rgba(229,57,53,0.15)"),
        ]
        for label, cor, fill_cor in series_ev:
            if label in df_ev.columns:
                fig_ev.add_trace(go.Scatter(
                    x=df_ev["mes"], y=df_ev[label],
                    mode="lines+markers", name=label,
                    line=dict(color=cor, width=2.5),
                    fill="tozeroy", fillcolor=fill_cor,
                    hovertemplate=f"<b>{label}</b>: %{{y:,.1f}} m³<extra></extra>"))
        fig_ev.update_layout(
            yaxis=dict(title="m³", tickformat=",.0f"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            margin=dict(t=50, b=20), height=360, hovermode="x unified")
        with ev1:
            st.plotly_chart(fig_ev, use_container_width=True, key="chart_prod_ev")

        st.divider()

        # ── Bloco C — Situação por obra ────────────────────
        st.markdown("#### 🏗️ Situação por obra")

        fab_obra = (df_fab_f.groupby("obra_id")["volume_total"].sum()
                    if not df_fab_f.empty else pd.Series(dtype=float))
        tra_obra = (df_tra_f.groupby("obra_id")["volume_real"].sum()
                    if not df_tra_f.empty else pd.Series(dtype=float))
        mon_obra = (df_mon_f.groupby("obra_id")["volume_total"].sum()
                    if not df_mon_f.empty else pd.Series(dtype=float))

        todas_ids = sorted(set(
            list(fab_obra.index) + list(tra_obra.index) + list(mon_obra.index)))
        rows_obra = []
        for oid in todas_ids:
            fab = fab_obra.get(oid, 0)
            exp = tra_obra.get(oid, 0)
            mon = mon_obra.get(oid, 0)
            gap = max(0.0, fab - exp)
            if   gap > 500: farol = "🔴"
            elif gap > 200: farol = "🟡"
            else:           farol = "🟢"
            rows_obra.append({
                "Obra":       obras_map.get(oid, f"ID {oid}"),
                "Fab. m³":    round(fab, 1),
                "% Expedido": min(round(exp / fab * 100, 1) if fab else 0, 100.0),
                "% Montado":  min(round(mon / fab * 100, 1) if fab else 0, 100.0),
                "Pátio m³":   round(gap, 1),
                "Pátio":      f"{farol} {gap:,.0f} m³".replace(",", "."),
            })

        df_obra = pd.DataFrame(rows_obra).sort_values("Fab. m³", ascending=False)
        if not df_obra.empty:
            st.dataframe(
                df_obra[["Obra", "Fab. m³", "% Expedido", "% Montado", "Pátio"]],
                use_container_width=True, hide_index=True,
                height=min(500, 36 + 35 * len(df_obra)),
                column_config={
                    "Obra":       st.column_config.TextColumn("Obra", width="large"),
                    "Fab. m³":    st.column_config.NumberColumn("Fabricado m³", format="%.1f"),
                    "% Expedido": st.column_config.ProgressColumn("% Expedido",
                                      min_value=0, max_value=100, format="%.1f%%"),
                    "% Montado":  st.column_config.ProgressColumn("% Montado",
                                      min_value=0, max_value=100, format="%.1f%%"),
                    "Pátio":      st.column_config.TextColumn("Pátio (gap)"),
                })
        st.caption("🔴 > 500 m³ no pátio   🟡 200–500 m³   🟢 < 200 m³")

        st.divider()

        # ── Bloco D — Composição da produção ───────────────
        st.markdown("#### 🔍 Composição da produção")
        l4a, l4b = st.columns(2)

        with l4a:
            st.subheader("Top 10 produtos fabricados (m³)")
            if not df_fab_f.empty:
                top_prod = (df_fab_f.groupby("produto")["volume_total"]
                            .sum().sort_values(ascending=True).tail(10).reset_index())
                top_prod.columns = ["produto", "volume"]
                top_prod = top_prod[top_prod["volume"] > 0]
                if not top_prod.empty:
                    fig_prod = go.Figure(go.Bar(
                        x=top_prod["volume"], y=top_prod["produto"],
                        orientation="h", marker_color="#1976D2",
                        hovertemplate="%{y}<br>%{x:,.1f} m³<extra></extra>"))
                    fig_prod.update_layout(
                        xaxis=dict(title="m³", tickformat=",.0f"),
                        yaxis=dict(autorange="reversed"),
                        margin=dict(t=20, b=20, r=20), height=400)
                    st.plotly_chart(fig_prod, use_container_width=True, key="chart_top_prod")
            else:
                st.info("Sem dados de fabricação.")

        with l4b:
            st.subheader("Volume por etapa construtiva")
            if not df_fab_f.empty:
                etapa_vol = (df_fab_f.groupby("etapa")["volume_total"]
                             .sum().sort_values(ascending=False).reset_index())
                etapa_vol = etapa_vol[etapa_vol["volume_total"] > 0]
                if not etapa_vol.empty:
                    fig_etapa = go.Figure(go.Pie(
                        labels=etapa_vol["etapa"],
                        values=etapa_vol["volume_total"],
                        hole=0.4,
                        textinfo="label+percent",
                        textposition="outside",
                        hovertemplate="%{label}<br>%{value:,.1f} m³<br>%{percent}<extra></extra>"))
                    fig_etapa.update_layout(
                        showlegend=False,
                        margin=dict(t=30, b=30, l=30, r=30), height=420)
                    st.plotly_chart(fig_etapa, use_container_width=True, key="chart_etapa")
            else:
                st.info("Sem dados de fabricação.")

    # ════════════════════════════════════════════════════════
    # ABA 2 — EFICIÊNCIA (R$/m³)
    # ════════════════════════════════════════════════════════
    with tab2:
        with st.spinner("Carregando custos..."):
            df_cp = carregar_custos_prod()

        if df_cp.empty:
            st.warning("Nenhum dado de custos disponível.")
        else:
            centros_disp = sorted(df_cp["centro_custos"].dropna().unique())
            default_centro = [c for c in centros_disp
                              if "PRODU" in str(c).upper()][:1] or centros_disp[:1]

            ef1, ef2 = st.columns([4, 3])
            centro_sel = ef1.multiselect(
                "Centro de custo", centros_disp, default=default_centro, key="ef_centro")

            df_cp_f = df_cp[df_cp["centro_custos"].isin(centro_sel)].copy() if centro_sel else df_cp.copy()
            if inicio_str:
                df_cp_f = df_cp_f[df_cp_f["data"] >= inicio_str]
            if fim_str:
                df_cp_f = df_cp_f[df_cp_f["data"] <= fim_str]

            # Volume fabricado por mês (filtrado por obras)
            vol_mes_fab = (df_fab_f.groupby("mes")["volume_total"].sum()
                           if not df_fab_f.empty else pd.Series(dtype=float))

            # Custo mensal (abs — despesas em negativo no sistema)
            custo_mes = df_cp_f.groupby("mes")["valor_global"].sum().abs()

            # Merge mês a mês
            meses_uniao = sorted(set(list(custo_mes.index) + list(vol_mes_fab.index)))
            df_ef = pd.DataFrame({"mes": meses_uniao})
            df_ef["custo"]  = df_ef["mes"].map(custo_mes).fillna(0)
            df_ef["volume"] = df_ef["mes"].map(vol_mes_fab).fillna(0)
            df_ef["rpm3"]   = df_ef.apply(
                lambda r: r["custo"] / r["volume"] if r["volume"] > 0 else None, axis=1)
            df_ef = df_ef[df_ef["mes"].notna() & ~df_ef["mes"].isin(["NaT", "nan"])]
            df_ef = df_ef.sort_values("mes").reset_index(drop=True)
            df_ef_valid = df_ef[df_ef["rpm3"].notna()].copy()

            if df_ef_valid.empty:
                st.info("Sem dados suficientes para R$/m³. "
                        "Verifique se há volume fabricado e custos no período selecionado.")
            else:
                media_rpm3 = df_ef_valid["rpm3"].mean()
                std_rpm3   = df_ef_valid["rpm3"].std() or 0.0
                ult = df_ef_valid.iloc[-1]
                ant = df_ef_valid.iloc[-2] if len(df_ef_valid) > 1 else None

                custo_ult = ult["custo"]
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("R$/m³ atual",
                          f"R$ {ult['rpm3']:,.0f}".replace(",", "."))
                if ant is not None:
                    c2.metric("R$/m³ mês anterior",
                              f"R$ {ant['rpm3']:,.0f}".replace(",", "."))
                    delta_pct = (ult["rpm3"] - ant["rpm3"]) / ant["rpm3"] * 100
                    c3.metric("Variação vs anterior", f"{delta_pct:+.1f}%",
                              delta_color="inverse")
                else:
                    c2.metric("R$/m³ mês anterior", "—")
                    c3.metric("Variação vs anterior", "—")
                media_hist_pct = ((ult["rpm3"] - media_rpm3) / media_rpm3 * 100
                                  if media_rpm3 else 0)
                c4.metric("Δ vs média histórica", f"{media_hist_pct:+.1f}%",
                          delta_color="inverse",
                          help=f"Média histórica: R$ {media_rpm3:,.0f}".replace(",", "."))
                c5.metric("Volume mês atual", fmt_m3(ult["volume"]))

                st.divider()

                # ── Gráfico R$/m³ com banda de referência ──────────
                st.markdown("#### 📉 R$/m³ ao longo do tempo")
                xs = df_ef_valid["mes"].tolist()
                ys = df_ef_valid["rpm3"].tolist()
                banda_sup = [media_rpm3 + std_rpm3] * len(xs)
                banda_inf = [max(0, media_rpm3 - std_rpm3)] * len(xs)

                fig_ef = go.Figure()
                fig_ef.add_trace(go.Scatter(
                    x=xs + xs[::-1],
                    y=banda_sup + banda_inf[::-1],
                    fill="toself", fillcolor="rgba(25,118,210,0.10)",
                    line=dict(color="rgba(0,0,0,0)"),
                    name="Faixa ±1σ", hoverinfo="skip"))
                fig_ef.add_trace(go.Scatter(
                    x=xs, y=[media_rpm3] * len(xs),
                    mode="lines",
                    name=f"Média R$ {media_rpm3:,.0f}".replace(",", "."),
                    line=dict(color="#1976D2", width=1, dash="dash"),
                    hoverinfo="skip"))
                cores_pts = ["#E53935" if r > media_rpm3 else "#43A047" for r in ys]
                fig_ef.add_trace(go.Scatter(
                    x=xs, y=ys,
                    mode="lines+markers", name="R$/m³",
                    line=dict(color="#37474F", width=2),
                    marker=dict(color=cores_pts, size=8),
                    hovertemplate="<b>%{x}</b><br>R$/m³: R$ %{y:,.0f}<extra></extra>"))
                fig_ef.update_layout(
                    yaxis=dict(title="R$/m³", tickformat=",.0f"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                    margin=dict(t=50, b=20), height=360, hovermode="x unified")
                st.plotly_chart(fig_ef, use_container_width=True, key="chart_rpm3")

                st.divider()

                # ── Decomposição Operacional vs Encargos ───────────
                st.markdown("#### 👷 Decomposição do custo pessoal")

                def classif_conta(conta):
                    if pd.isna(conta): return "Outros"
                    c = str(conta).upper()
                    if any(k in c for k in ["SALÁRIO", "SALARIO", "HORA EXTRA",
                                             "HORAS EXTRAS", "SALÁRIOS"]):
                        return "Operacional"
                    if any(k in c for k in ["FÉRIAS", "FERIAS", "13", "INSS",
                                             "FGTS", "ALIMENTAÇÃO", "ALIMENTACAO",
                                             "ENCARGO"]):
                        return "Encargos"
                    return "Outros"

                df_cp_f = df_cp_f.copy()
                df_cp_f["categoria"] = df_cp_f["conta_gerencial"].apply(classif_conta)
                custo_cat_mes = (df_cp_f.groupby(["mes", "categoria"])["valor_global"]
                                 .sum().abs().reset_index())
                meses_ef = df_ef_valid["mes"].tolist()
                custo_cat_mes = custo_cat_mes[custo_cat_mes["mes"].isin(meses_ef)]

                fig_dec = go.Figure()
                cores_cat = {"Operacional": "#1976D2", "Encargos": "#FBC02D",
                             "Outros": "#78909C"}
                for cat in ["Operacional", "Encargos", "Outros"]:
                    df_cat = custo_cat_mes[custo_cat_mes["categoria"] == cat]
                    df_cat_full = (pd.DataFrame({"mes": meses_ef})
                                   .merge(df_cat[["mes", "valor_global"]], on="mes", how="left")
                                   .fillna(0))
                    if df_cat_full["valor_global"].sum() == 0:
                        continue
                    fig_dec.add_trace(go.Bar(
                        x=df_cat_full["mes"], y=df_cat_full["valor_global"],
                        name=cat, marker_color=cores_cat[cat],
                        hovertemplate=f"<b>{cat}</b><br>%{{x}}: R$ %{{y:,.0f}}<extra></extra>"))
                fig_dec.update_layout(
                    barmode="stack",
                    yaxis=dict(title="R$", tickformat=",.0f"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                    margin=dict(t=50, b=20), height=320)
                st.plotly_chart(fig_dec, use_container_width=True, key="chart_decomp")

                st.divider()

                # ── Tabela mensal detalhada ─────────────────────────
                st.markdown("#### 📋 Tabela mensal detalhada")

                def classe_rpm3(v):
                    if pd.isna(v) or media_rpm3 == 0: return "—"
                    desvio = (v - media_rpm3) / media_rpm3 * 100
                    if desvio <= 0:  return "🟢 Bom"
                    if desvio <= 20: return "🟡 Atenção"
                    return "🔴 Ruim"

                df_tbl = df_ef_valid.copy()
                df_tbl["vs_media"] = df_tbl["rpm3"].apply(
                    lambda v: f"{(v - media_rpm3) / media_rpm3 * 100:+.0f}%"
                    if not pd.isna(v) and media_rpm3 else "—")
                df_tbl["Classe"]     = df_tbl["rpm3"].apply(classe_rpm3)
                df_tbl["R$/m³"]      = df_tbl["rpm3"].apply(
                    lambda v: f"R$ {v:,.0f}".replace(",", ".") if not pd.isna(v) else "—")
                df_tbl["Custo"]      = df_tbl["custo"].apply(
                    lambda v: f"R$ {v:,.0f}".replace(",", "."))
                df_tbl["Volume m³"]  = df_tbl["volume"].apply(
                    lambda v: f"{v:,.1f}".replace(",", "."))
                st.dataframe(
                    df_tbl[["mes", "Volume m³", "Custo", "R$/m³", "vs_media", "Classe"]]
                          .rename(columns={"mes": "Mês", "vs_media": "vs Média"})
                          .sort_values("Mês", ascending=False),
                    use_container_width=True, hide_index=True,
                    height=min(500, 36 + 35 * len(df_tbl)))

                st.divider()

                # ── Heatmap de sazonalidade ─────────────────────────
                st.markdown("#### 🗓️ Sazonalidade — R$/m³ por mês/ano")
                df_hm = df_ef_valid.copy()
                df_hm["data_hm"] = pd.to_datetime(df_hm["mes"], errors="coerce")
                df_hm = df_hm.dropna(subset=["data_hm"])
                df_hm["ano"]    = df_hm["data_hm"].dt.year.astype(str)
                df_hm["mes_nm"] = df_hm["data_hm"].dt.month

                if not df_hm.empty:
                    pivot = df_hm.pivot_table(
                        index="ano", columns="mes_nm",
                        values="rpm3", aggfunc="mean")
                    pivot.columns = [f"{m:02d}" for m in pivot.columns]
                    pivot = pivot.sort_index(ascending=False)
                    nomes_mes = {f"{i:02d}": n for i, n in enumerate(
                        ["Jan","Fev","Mar","Abr","Mai","Jun",
                         "Jul","Ago","Set","Out","Nov","Dez"], 1)}
                    pivot.columns = [nomes_mes.get(c, c) for c in pivot.columns]
                    fig_hm = px.imshow(
                        pivot, text_auto=".0f",
                        color_continuous_scale=["#43A047", "#FFEE58", "#E53935"],
                        aspect="auto",
                        labels=dict(color="R$/m³"))
                    fig_hm.update_layout(
                        xaxis_title="Mês", yaxis_title="Ano",
                        margin=dict(t=20, b=20),
                        height=max(180, 70 * len(pivot)))
                    st.plotly_chart(fig_hm, use_container_width=True, key="chart_heatmap")

    # ════════════════════════════════════════════════════════
    # ABA 3 — INSUMOS
    # ════════════════════════════════════════════════════════
    with tab3:
        vol_total_fab = df_fab_f["volume_total"].sum() if not df_fab_f.empty else 0

        # Totais por tipo de aço
        if not df_fab_f.empty:
            aco_frouxo    = df_fab_f["peso_aco_frouxo"].sum()
            cord_viga     = df_fab_f["cord_viga"].sum()
            cord_laje     = df_fab_f["cord_laje"].sum()
            cord_outros   = df_fab_f["cord_outros"].sum()
            aco_total     = df_fab_f["peso_aco"].sum()
            tem_breakdown = (aco_frouxo + cord_viga + cord_laje + cord_outros) > 0
        else:
            aco_frouxo = cord_viga = cord_laje = cord_outros = aco_total = 0
            tem_breakdown = False

        kgm3 = aco_total / vol_total_fab if vol_total_fab > 0 else 0

        with st.spinner("Carregando dados do pátio..."):
            df_patio = carregar_patio(inicio_str, fim_str)

        if obra_ids_sel and not df_patio.empty:
            df_patio = df_patio[df_patio["obra_id"].isin(obra_ids_sel)]

        prazo_medio = df_patio["dias_patio"].mean() if not df_patio.empty else None

        # ── Bloco A — métricas de aço ───────────────────────
        st.markdown("#### ⚙️ Consumo de aço")
        if tem_breakdown:
            pct_frouxo = aco_frouxo / aco_total * 100 if aco_total else 0
            pct_cv     = cord_viga  / aco_total * 100 if aco_total else 0
            pct_cl     = cord_laje  / aco_total * 100 if aco_total else 0
            i1, i2, i3, i4, i5 = st.columns(5)
            i1.metric("⚙️ Aço total",
                      f"{aco_total:,.0f} kg".replace(",", "."))
            i2.metric("🔩 Frouxo",
                      f"{aco_frouxo:,.0f} kg".replace(",", "."),
                      delta=f"{pct_frouxo:.0f}% do total", delta_color="off")
            i3.metric("🔗 Cord. Viga",
                      f"{cord_viga:,.0f} kg".replace(",", "."),
                      delta=f"{pct_cv:.0f}% do total", delta_color="off")
            i4.metric("🔗 Cord. Laje",
                      f"{cord_laje:,.0f} kg".replace(",", "."),
                      delta=f"{pct_cl:.0f}% do total", delta_color="off")
            i5.metric("📐 kg aço / m³", f"{kgm3:.2f} kg/m³")
        else:
            i1, i2, i3 = st.columns(3)
            i1.metric("⚙️ Aço total consumido",
                      f"{aco_total:,.0f} kg".replace(",", "."))
            i2.metric("📐 kg aço / m³", f"{kgm3:.2f} kg/m³")
            i3.metric("⏱️ Prazo médio pátio",
                      f"{prazo_medio:.1f} dias" if prazo_medio is not None else "—")

        st.divider()

        # ── Bloco B — Evolução mensal dos tipos de aço ─────
        st.markdown("#### 📈 Evolução mensal do consumo de aço")
        if not df_fab_f.empty and "mes" in df_fab_f.columns:
            if tem_breakdown:
                aco_mes = (df_fab_f.groupby("mes")
                           .agg(frouxo=("peso_aco_frouxo",  "sum"),
                                cv=("cord_viga",            "sum"),
                                cl=("cord_laje",            "sum"),
                                co=("cord_outros",          "sum"))
                           .reset_index().sort_values("mes"))
                fig_aco_ev = go.Figure()
                series_aco = [
                    ("frouxo", "Frouxo",         "#1976D2"),
                    ("cv",     "Cord. Viga",     "#FB8C00"),
                    ("cl",     "Cord. Laje",     "#43A047"),
                    ("co",     "Cord. Outros",   "#9C27B0"),
                ]
                for col, nome, cor in series_aco:
                    if aco_mes[col].sum() == 0:
                        continue
                    fig_aco_ev.add_trace(go.Scatter(
                        x=aco_mes["mes"], y=aco_mes[col],
                        mode="lines+markers", name=nome,
                        line=dict(color=cor, width=2),
                        hovertemplate=f"<b>{nome}</b>: %{{y:,.0f}} kg<extra></extra>"))
            else:
                aco_mes = (df_fab_f.groupby("mes")["peso_aco"].sum()
                           .reset_index().rename(columns={"peso_aco": "total"})
                           .sort_values("mes"))
                fig_aco_ev = go.Figure()
                fig_aco_ev.add_trace(go.Scatter(
                    x=aco_mes["mes"], y=aco_mes["total"],
                    mode="lines+markers", name="Aço total",
                    line=dict(color="#E53935", width=2),
                    hovertemplate="<b>%{x}</b>: %{y:,.0f} kg<extra></extra>"))
            fig_aco_ev.update_layout(
                yaxis=dict(title="kg", tickformat=",.0f"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=50, b=20), height=320, hovermode="x unified")
            st.plotly_chart(fig_aco_ev, use_container_width=True, key="chart_aco_ev")
        else:
            st.info("Sem dados de aço no período.")

        st.divider()

        # ── Bloco C — Benchmark kg/m³ por produto ──────────
        st.markdown("#### 📊 Benchmark kg aço/m³ por produto")
        if not df_fab_f.empty:
            # Carregar todos os dados históricos (sem filtro temporal) para média
            @st.cache_data(ttl=600)
            def carregar_fabricacao_historico():
                def _q(cols):
                    rows, page, size = [], 0, 1000
                    q = supabase.table("producao_fabricacao").select(cols)
                    while True:
                        resp = q.range(page * size, (page + 1) * size - 1).execute()
                        rows.extend(resp.data)
                        if len(resp.data) < size: break
                        page += 1
                    return rows
                try:
                    rows = _q("obra_id, produto, volume_total, peso_aco,"
                              " peso_aco_frouxo, peso_aco_protendido")
                except Exception:
                    rows = _q("obra_id, produto, volume_total, peso_aco")
                df = pd.DataFrame(rows)
                if df.empty: return df
                for col in ["volume_total", "peso_aco", "peso_aco_frouxo", "peso_aco_protendido"]:
                    if col not in df.columns: df[col] = 0.0
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                def _tipo(n):
                    n = str(n).upper()
                    if "LAJE" in n: return "Laje"
                    if "VIGA" in n: return "Viga"
                    return "Outros"
                df["tipo_produto"] = df["produto"].apply(_tipo)
                df["cord_viga"]   = df["peso_aco_protendido"].where(df["tipo_produto"] == "Viga",  0)
                df["cord_laje"]   = df["peso_aco_protendido"].where(df["tipo_produto"] == "Laje",  0)
                df["cord_outros"] = df["peso_aco_protendido"].where(df["tipo_produto"] == "Outros", 0)
                return df

            df_hist = carregar_fabricacao_historico()

            # Média histórica kg/m³ por produto
            if not df_hist.empty:
                ref_prod = (df_hist[df_hist["volume_total"] > 0]
                            .groupby("produto")
                            .agg(aco_h=("peso_aco", "sum"), vol_h=("volume_total", "sum"))
                            .reset_index())
                ref_prod["ref_kgm3"] = ref_prod["aco_h"] / ref_prod["vol_h"]
            else:
                ref_prod = pd.DataFrame(columns=["produto", "ref_kgm3"])

            bench = (df_fab_f[df_fab_f["volume_total"] > 0]
                     .groupby("produto")
                     .agg(frouxo=("peso_aco_frouxo", "sum"),
                          cv=("cord_viga",           "sum"),
                          cl=("cord_laje",           "sum"),
                          aco=("peso_aco",           "sum"),
                          vol=("volume_total",       "sum"))
                     .reset_index())
            bench["frouxo_m3"] = bench["frouxo"] / bench["vol"]
            bench["cv_m3"]     = bench["cv"]     / bench["vol"]
            bench["cl_m3"]     = bench["cl"]     / bench["vol"]
            bench["total_m3"]  = bench["aco"]    / bench["vol"]
            bench = bench.merge(ref_prod[["produto", "ref_kgm3"]], on="produto", how="left")
            bench["vs_ref"] = bench.apply(
                lambda r: f"{(r['total_m3'] - r['ref_kgm3']) / r['ref_kgm3'] * 100:+.0f}%"
                if pd.notna(r["ref_kgm3"]) and r["ref_kgm3"] > 0 else "—", axis=1)

            def _farol_bench(row):
                if pd.isna(row["ref_kgm3"]) or row["ref_kgm3"] == 0: return "—"
                pct = (row["total_m3"] - row["ref_kgm3"]) / row["ref_kgm3"] * 100
                if pct > 20: return "🔴 Alto"
                if pct > 5:  return "🟡 Médio"
                return "🟢 Normal"

            bench["Status"] = bench.apply(_farol_bench, axis=1)
            bench = bench.sort_values("total_m3", ascending=False)

            if tem_breakdown:
                df_bench_show = bench[["produto", "frouxo_m3", "cv_m3", "cl_m3",
                                       "total_m3", "vs_ref", "Status"]].copy()
                df_bench_show.columns = ["Produto", "Frouxo kg/m³", "Cord.Viga kg/m³",
                                          "Cord.Laje kg/m³", "Total kg/m³", "vs Hist.", "Status"]
            else:
                df_bench_show = bench[["produto", "total_m3", "vs_ref", "Status"]].copy()
                df_bench_show.columns = ["Produto", "kg/m³", "vs Hist.", "Status"]

            st.dataframe(
                df_bench_show,
                use_container_width=True, hide_index=True,
                height=min(500, 36 + 35 * len(df_bench_show)))

        st.divider()

        # ── Bloco D — Pizza de composição + Prazo pátio ────
        col_pizza, col_patio_bloco = st.columns(2)

        with col_pizza:
            st.markdown("#### 🥧 Composição do aço")
            if not df_fab_f.empty and tem_breakdown and aco_total > 0:
                _raw = [
                    ("Frouxo",       aco_frouxo,  "#1976D2"),
                    ("Cord. Viga",   cord_viga,   "#FB8C00"),
                    ("Cord. Laje",   cord_laje,   "#43A047"),
                    ("Cord. Outros", cord_outros, "#9C27B0"),
                ]
                _labels = [l for l, v, _ in _raw if v > 0]
                _values = [v for _, v, _ in _raw if v > 0]
                _colors = [c for _, v, c in _raw if v > 0]
                fig_pizza = go.Figure(go.Pie(
                    labels=_labels, values=_values,
                    hole=0.45,
                    marker_colors=_colors,
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>%{value:,.0f} kg<br>%{percent}<extra></extra>"))
                fig_pizza.update_layout(
                    showlegend=False,
                    margin=dict(t=20, b=20, l=20, r=20), height=340)
                st.plotly_chart(fig_pizza, use_container_width=True, key="chart_pizza_aco")
            elif not df_fab_f.empty and aco_total > 0:
                st.info("Colunas peso_aco_frouxo / peso_aco_protendido sem dados no período.")
            else:
                st.info("Sem dados de aço.")

        with col_patio_bloco:
            st.markdown("#### 🏗️ Aço por m³ por produto (Top 20)")
            if not df_fab_f.empty:
                aco_prod = (df_fab_f.groupby("produto")
                            .agg(aco=("peso_aco", "sum"), vol=("volume_total", "sum"))
                            .reset_index())
                aco_prod = aco_prod[aco_prod["vol"] > 0].copy()
                aco_prod["kgm3"] = aco_prod["aco"] / aco_prod["vol"]
                aco_prod = (aco_prod[aco_prod["kgm3"] > 0]
                            .sort_values("kgm3", ascending=True).tail(20))
                if not aco_prod.empty:
                    fig_aco_p = go.Figure(go.Bar(
                        x=aco_prod["kgm3"], y=aco_prod["produto"],
                        orientation="h", marker_color="#E53935",
                        hovertemplate="%{y}<br>%{x:.2f} kg/m³<extra></extra>"))
                    fig_aco_p.update_layout(
                        xaxis=dict(title="kg aço / m³"),
                        yaxis=dict(autorange="reversed"),
                        margin=dict(t=20, b=20, r=20), height=420)
                    st.plotly_chart(fig_aco_p, use_container_width=True, key="chart_aco_prod")
            else:
                st.info("Sem dados de fabricação.")

        st.divider()

        # ── Bloco E — Prazo no pátio ────────────────────────
        st.markdown("#### ⏱️ Prazo no pátio (Fab → Expedição)")
        col_pat, col_hist = st.columns(2)

        with col_pat:
            st.markdown("##### Por obra")
            if not df_patio.empty:
                prazo_obra = (df_patio.groupby("obra_id")["dias_patio"]
                              .mean().reset_index())
                prazo_obra["Obra"] = prazo_obra["obra_id"].map(obras_map).fillna(
                    prazo_obra["obra_id"].astype(str))
                prazo_obra = prazo_obra.sort_values("dias_patio", ascending=False)

                def _status_patio(d):
                    if d <= 10: return "🟢 Normal"
                    if d <= 20: return "🟡 Atenção"
                    return "🔴 Crítico"

                prazo_obra["Status"] = prazo_obra["dias_patio"].apply(_status_patio)
                prazo_obra["Dias"]   = prazo_obra["dias_patio"].round(1)
                st.dataframe(
                    prazo_obra[["Obra", "Dias", "Status"]],
                    use_container_width=True, hide_index=True,
                    height=min(420, 36 + 35 * len(prazo_obra)))
                st.caption("🔴 > 20 dias   🟡 10–20 dias   🟢 < 10 dias")
            else:
                st.info("Sem dados de prazo no pátio.")

        with col_hist:
            st.markdown("##### Distribuição de peças")
            if not df_patio.empty:
                bins   = [0, 7, 15, 30, 60, float("inf")]
                labels = ["0–7 dias", "7–15 dias", "15–30 dias", "30–60 dias", "+60 dias"]
                dias   = df_patio["dias_patio"].clip(lower=0)
                conts  = (pd.cut(dias, bins=bins, labels=labels, right=False)
                          .value_counts().reindex(labels))
                fig_hist = go.Figure(go.Bar(
                    x=conts.index, y=conts.values,
                    marker_color=["#43A047", "#8BC34A", "#FBC02D", "#FB8C00", "#E53935"],
                    hovertemplate="%{x}<br>%{y} peças<extra></extra>"))
                fig_hist.update_layout(
                    xaxis_title="Faixa de prazo",
                    yaxis_title="Peças",
                    margin=dict(t=20, b=20), height=340)
                st.plotly_chart(fig_hist, use_container_width=True, key="chart_hist_patio")
            else:
                st.info("Sem dados de pátio.")