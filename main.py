import streamlit as st
import pandas as pd
import numpy as np
import re
from supabase import create_client
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================
st.set_page_config(page_title="Civil Gestão", page_icon="🏛️", layout="wide")

@st.cache_resource
def iniciar_conexao():
    import os
    url   = (st.secrets.get("SUPABASE_URL")
             or os.environ.get("SUPABASE_URL"))
    chave = (st.secrets.get("SUPABASE_KEY")
             or os.environ.get("SUPABASE_KEY"))
    if not url or not chave:
        st.error("⚠️ Credenciais do Supabase não configuradas.")
        st.stop()
    return create_client(url, chave)

supabase = iniciar_conexao()

# ==========================================================
# NAVEGAÇÃO
# ==========================================================
st.sidebar.title("🏛️ CIVIL GESTÃO")
st.sidebar.caption("OBRAS & MEDIÇÕES")
st.sidebar.divider()

pagina_selecionada = st.sidebar.radio("Menu Principal", [
    "--- 📊 DASHBOARDS ---", "🏭 Produção", "💰 Financeiro", "💸 Custos & Despesas",
    "🔍 Análise da Obra", "👷 Folha / RH",
    "--- 🛠️ GESTÃO ---", "🏗️ Gestão de Obras", "🛤️ Jornada da Obra", "👥 Equipe",
    "📋 Gestão à Vista", "👤 Reunião 1:1",
    "--- 🗄️ BASE DE DADOS ---", "✏️ Editar Obras", "📥 Importador de Arquivos"
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
            if _math.isnan(r) or _math.isinf(r) or abs(r) >= 1e13:
                return None
            return r
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

@st.cache_data(ttl=600)
def mapa_obras():
    """Dicionário código-4-dígitos → id do banco."""
    dados = supabase.table("obras").select("id, cod4").execute().data
    m = {}
    for o in dados:
        c = str(o["cod4"]).strip()[:4] if o.get("cod4") else None
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

@st.cache_data(ttl=600)
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

def enviar_lotes(tabela, pacote, barra_label="Enviando...", on_conflict=None,
                 truncate_before=False):
    """Envia em lotes de 500 com barra de progresso.
    truncate_before=True → DELETE todos antes de inserir (reimportação limpa).
    on_conflict → upsert; sem on_conflict → insert."""
    if truncate_before:
        supabase.table(tabela).delete().neq("id", -1).execute()
    total = len(pacote)
    if total == 0:
        return 0
    barra = st.progress(0, text=barra_label)
    enviado = 0
    for i in range(0, total, 500):
        lote = pacote[i:i+500]
        try:
            if on_conflict:
                supabase.table(tabela).upsert(lote, on_conflict=on_conflict).execute()
            else:
                supabase.table(tabela).insert(lote).execute()
        except Exception as _batch_err:
            raise RuntimeError(
                f"Falha no lote {i//500 + 1} (registros {i}–{i+len(lote)-1}): {_batch_err}"
            ) from _batch_err
        enviado += len(lote)
        barra.progress(min(enviado/total, 1.0), text=f"{enviado}/{total} registros")
    return enviado

# ==========================================================
# IMPORTADOR
# ==========================================================
if pagina_selecionada == "📥 Importador de Arquivos":
    st.header("📥 Central de Importação")

    import math as _math
    import traceback as _tb

    # ── helpers locais ────────────────────────────────────────────────────────
    def _limpar_num_imp(val):
        """Converte string numérica para float.
        Detecta automaticamente formato BR (vírgula decimal) e US (ponto decimal).
        Exemplos: 'R$ 1.234,56' → 1234.56 | '1234.56' → 1234.56 | '1.234.567,89' → 1234567.89
        """
        if val is None or str(val).strip().lower() in ("", "nan", "nat", "none", "inf"):
            return None
        s = str(val).strip().replace("R$", "").replace(" ", "").strip()
        if not s:
            return None
        try:
            last_dot   = s.rfind(".")
            last_comma = s.rfind(",")
            if last_dot >= 0 and last_comma >= 0:
                # Ambos presentes: o último é o separador decimal
                if last_comma > last_dot:
                    # Formato BR: 1.234,56 → remove pontos, troca vírgula
                    s = s.replace(".", "").replace(",", ".")
                else:
                    # Formato US: 1,234.56 → remove vírgulas
                    s = s.replace(",", "")
            elif last_comma >= 0:
                # Só vírgula: trata como decimal se ≤ 3 dígitos depois dela
                digits_after = len(s) - last_comma - 1
                if digits_after <= 3:
                    s = s.replace(",", ".")
                else:
                    s = s.replace(",", "")
            # Só ponto (ou nenhum): float() interpreta corretamente
            v = float(s)
            return None if (_math.isnan(v) or _math.isinf(v)) else v
        except Exception:
            return None

    def _limpar_str_imp(val):
        if val is None or str(val).strip().lower() in ("", "nan", "nat", "none"):
            return None
        return str(val).strip()

    def _inline_cache_clear():
        limpar_todos_caches()

    # ── seleção de rota ───────────────────────────────────────────────────────
    opcao = st.selectbox("Qual arquivo está importando?", [
        "1. Equipe              → qualquer CSV com colunas nome / email / setor / cargo",
        "2. Financeiro Obras    → Civil_Comercial_*.csv",
        "3. Medições            → Lista_Medição.csv",
        "4. Fabricação          → exportação ERP peça a peça",
        "5. Transporte          → exportação ERP expedição",
        "6. Montagem            → exportação ERP montagem",
        "7. Custos              → exportação XLSX do sistema RM",
        "8. Folha / RH          → exportação XLSX mensal de folha de pagamento",
        "9. Receitas (RM)       → exportação XLSX do sistema RM",
        "10. Despesas (RM)      → exportação XLSX do sistema RM",
    ])
    # Extrai o número da rota (suporta 2 dígitos: "10")
    rota = opcao.strip().split(".")[0].strip()

    arquivo = st.file_uploader("Arraste o arquivo aqui", type=["csv", "xlsx"])

    if arquivo:
        # ── leitura automática (sep ; ou ,) ───────────────────────────────────
        if rota in ("7", "8", "9", "10"):
            # XLSX — pré-visualização via openpyxl
            try:
                arquivo.seek(0)
                df_prev = pd.read_excel(arquivo, engine="openpyxl", dtype=str)
                with st.expander(f"👁️ Pré-visualização — {df_prev.shape[0]} linhas × {df_prev.shape[1]} colunas"):
                    st.dataframe(df_prev.head(10), use_container_width=True)
            except Exception as e:
                st.error(f"❌ Erro ao ler XLSX: {e}")
                st.stop()
        else:
            try:
                arquivo.seek(0)
                df_prev = pd.read_csv(arquivo, sep=None, engine="python",
                                      encoding="utf-8-sig", dtype=str, header=0)
                with st.expander(f"👁️ Pré-visualização — {df_prev.shape[0]} linhas × {df_prev.shape[1]} colunas"):
                    st.dataframe(df_prev.head(10), use_container_width=True)
            except Exception as e:
                st.error(f"❌ Erro ao ler CSV: {e}")
                st.stop()

        # ── Correlação com Obras (rotas 4–7, 9, 10) ──────────────────────────
        if rota in ["4","5","6","7","9","10"]:
            arquivo.seek(0)
            if rota in ("7", "9", "10"):
                _df_corr = pd.read_excel(arquivo, engine="openpyxl", dtype=str)
            else:
                _df_corr = pd.read_csv(arquivo, sep=None, engine="python",
                                       encoding="utf-8-sig", dtype=str, header=0)
            _df_corr.columns = [c.strip() for c in _df_corr.columns]
            # Renomear para nomes canônicos conforme a rota
            if rota in ("7", "9", "10"):
                _cm7 = {
                    "Data":"data","IdLanç":"id_lancamento","NºDoc":"numero_doc",
                    "Centro de Custos":"centro_custos","Conta Macro":"conta_macro",
                    "Conta Gerencial":"conta_gerencial","Cli/For":"cli_fornecedor",
                    "Produto_Serviço":"produto_servico","Produto_Servico":"produto_servico",
                    "CriadoPor":"criado_por","Valor Global":"valor_global",
                    " Valor Global ":"valor_global","Qtd":"qtd",
                    "PrecoUnitario":"preco_unitario"," PrecoUnitario ":"preco_unitario",
                    "Origem":"origem","Chave_ColigadaIdOrigem":"chave_coligada",
                    "Cód. e Tipo Doc/Movimento":"cod_tipo_doc",
                }
                _df_corr = _df_corr.rename(columns=_cm7)
            else:
                _pn_map = {
                    "4": ["peca","codigo","obra_codigo","etapa","produto","secao",
                           "qtde_pecas","volume_total","data_fabricacao","volume_teorico",
                           "peso_aco","peso_aco_frouxo","peso_aco_protendido","comprimento"],
                    "5": ["peca","codigo","obra_codigo","etapa","produto","data_expedicao",
                           "volume_real","status","peso","numero_carga","transportadora",
                           "motorista","nota_fiscal"],
                    "6": ["peca","codigo","obra_codigo","etapa","produto","secao",
                           "qtde_pecas","volume_total","data_montagem","volume_teorico","peso"],
                }
                _pn = _pn_map[rota]
                _nn = min(len(_pn), len(_df_corr.columns))
                _df_corr.columns = _pn[:_nn] + list(_df_corr.columns[_nn:])
            # Detecta colunas com código de obra (4 dígitos)
            _cands_corr = []
            for _cc in _df_corr.columns:
                _ss = _df_corr[_cc].apply(extrair_codigo).dropna()
                if len(_ss) > 0:
                    _top_cod = _ss.value_counts().index[0]
                    _cands_corr.append((_cc, _top_cod, len(_ss)))
            with st.expander("🔗 Correlação com Obras", expanded=(rota in ("7","9","10"))):
                if _cands_corr:
                    st.dataframe(
                        pd.DataFrame(_cands_corr,
                                     columns=["Coluna no arquivo","Exemplo cod4","Nº linhas"]),
                        use_container_width=True, hide_index=True)
                    st.caption("O sistema vai usar a **primeira coluna da lista** como referência "
                               "de obra, salvo se você selecionar outra abaixo.")
                else:
                    st.warning("⚠️ Nenhuma coluna com código de 4 dígitos encontrada. "
                               "Use o campo abaixo para especificar manualmente.")
                _opts_corr = (["🔍 Automático"]
                              + [_cc for _cc, _, _ in _cands_corr]
                              + ["⛔ Nenhuma (sem vínculo com obra)"])
                st.selectbox("Coluna que contém o código da obra:",
                             options=_opts_corr, key="imp_col_obra_sel",
                             help="Selecione 'Automático' para usar a detecção padrão "
                                  "(primeira coluna da tabela acima).")
                st.text_input(
                    "Ou informe um cod4 fixo para TODAS as linhas (ex: 8198):",
                    key="imp_cod4_manual",
                    placeholder="Deixe vazio para usar a coluna selecionada acima",
                    help="Use quando todas as linhas do arquivo pertencem a uma única obra. "
                         "Se preenchido, tem prioridade sobre a seleção acima.")

        # ── Mês de competência (Rota 8) — fora do botão para não perder ao rerun
        if rota == "8":
            st.info("📅 Informe o mês de competência desta folha antes de importar.")
            mes_comp_r8 = st.date_input(
                "Mês de competência (selecione o dia 1 do mês)",
                value=None,
                key="folha_mes_comp",
                format="DD/MM/YYYY",
                help="Selecione o primeiro dia do mês referente a esta folha. Ex: 01/01/2025"
            )
            if not mes_comp_r8:
                st.warning("⚠️ Selecione o mês de competência para continuar.")

        if st.button("🚀 Importar", type="primary"):
            with st.spinner("Processando..."):
                try:

                    # ══ ROTA 1 — EQUIPE ══════════════════════════════════════
                    if rota == "1":
                        df1 = df_prev.copy()
                        # Aceita variações de nome de coluna
                        rename1 = {}
                        for c in df1.columns:
                            cl = c.lower().strip()
                            if cl in ("nome","name","colaborador"): rename1[c] = "nome"
                            elif cl in ("email","e-mail","e_mail"):  rename1[c] = "email"
                            elif cl in ("setor","area","departamento"): rename1[c] = "setor"
                            elif cl in ("cargo","funcao","função","role"): rename1[c] = "cargo"
                            elif cl in ("status","ativo"): rename1[c] = "status"
                        df1 = df1.rename(columns=rename1)
                        # Colunas obrigatórias
                        for col in ["nome"]:
                            if col not in df1.columns:
                                st.error(f"❌ Coluna obrigatória '{col}' não encontrada.")
                                st.stop()
                        # Opcionais: preenche None se ausentes
                        for col in ["email","setor","cargo","status"]:
                            if col not in df1.columns:
                                df1[col] = None
                        df1 = nulos(df1)
                        pacote1 = df1[["nome","email","setor","cargo","status"]].to_dict("records")
                        supabase.table("equipe").insert(pacote1).execute()
                        _inline_cache_clear()
                        st.success(f"🎉 {len(pacote1)} colaboradores importados!")

                    # ══ ROTA 2 — FINANCEIRO OBRAS ═════════════════════════════
                    elif rota == "2":
                        arquivo.seek(0)
                        df2 = pd.read_csv(arquivo, sep=None, engine="python",
                                          encoding="utf-8-sig", dtype=str, header=0)
                        nomes2 = [
                            "data_contrato", "codigo_produto", "obra_nome",
                            "volume", "faturamento_total", "faturamento_civil",
                            "faturamento_direto", "custo_total", "despesas_indiretas",
                            "impostos", "lucro", "cimento_cliente_ton", "cimento_civil_ton",
                            "chave_coligada", "razao_social", "cnpj", "responsavel",
                            "email", "_tipo_item", "_caminho",
                            "volume_projeto", "concreto", "aco_estrutural", "formas",
                            "mo_producao", "eps", "estuque", "projetos",
                            "descida_agua", "insertos", "consoles", "investimentos",
                            "materiais_consumo", "equip_fab", "custos_indiretos",
                            "pecas_consorcio", "frete", "equip_montagem", "mo_montagem",
                            "neoprene", "despesas_equipe", "topografia", "mobilizacao",
                            "equip_aux_montagem", "outros", "eventuais", "despesas_comerciais",
                            "_extra1", "_extra2", "_extra3",
                        ]
                        df2 = df2.rename(columns={
                            df2.columns[i]: nomes2[i]
                            for i in range(min(len(df2.columns), len(nomes2)))
                        })
                        _texto2 = {"data_contrato","codigo_produto","obra_nome",
                                   "chave_coligada","razao_social","cnpj","responsavel","email"}
                        _cols_num2 = [c for c in nomes2
                                      if c not in _texto2 and not c.startswith("_")
                                      and c in df2.columns]
                        cols_bd2 = [
                            "cod4","data_contrato","codigo_produto","obra_nome",
                            "chave_coligada","razao_social","cnpj","responsavel","email",
                            "volume","volume_projeto","faturamento_total","faturamento_civil",
                            "faturamento_direto","custo_total","despesas_indiretas",
                            "impostos","lucro","concreto","aco_estrutural","formas",
                            "mo_producao","eps","estuque","projetos","descida_agua",
                            "insertos","consoles","investimentos","neoprene",
                            "materiais_consumo","equip_fab","custos_indiretos",
                            "pecas_consorcio","frete","equip_montagem","mo_montagem",
                            "despesas_equipe","topografia","mobilizacao","equip_aux_montagem",
                            "outros","eventuais","despesas_comerciais",
                            "cimento_cliente_ton","cimento_civil_ton",
                        ]
                        criadas2 = inseridas2 = atualizadas2 = sem_cod2 = 0
                        barra2 = st.progress(0, text="Processando...")
                        tot2 = len(df2)
                        for idx2, row2 in df2.iterrows():
                            barra2.progress((idx2+1)/tot2, text=f"{idx2+1}/{tot2}")
                            cod4 = extrair_codigo(row2.get("obra_nome",""))
                            if not cod4:
                                sem_cod2 += 1; continue
                            nome_val = _limpar_str_imp(row2.get("obra_nome"))
                            r_obra = (supabase.table("obras").select("id,cod4,nome")
                                      .eq("cod4", cod4).execute())
                            if not r_obra.data:
                                ins2 = supabase.table("obras").insert({
                                    "cod4": cod4, "nome": nome_val, "status": "Em Andamento"
                                }).execute()
                                criadas2 += 1
                            else:
                                supabase.table("obras").update(
                                    {"nome": nome_val}
                                ).eq("cod4", cod4).execute()
                            payload2 = {"cod4": cod4}
                            payload2["data_contrato"] = formatar_data_valor(row2.get("data_contrato"))
                            for campo in ["codigo_produto","obra_nome","chave_coligada",
                                          "razao_social","cnpj","responsavel","email"]:
                                if campo in df2.columns:
                                    payload2[campo] = _limpar_str_imp(row2.get(campo))
                            for campo in _cols_num2:
                                if campo in df2.columns:
                                    payload2[campo] = parse_brl(row2.get(campo))
                            payload2 = {k: v for k, v in payload2.items() if k in cols_bd2}
                            r_fin2 = (supabase.table("obras_financeiro").select("cod4")
                                      .eq("cod4", cod4).execute())
                            if not r_fin2.data:
                                supabase.table("obras_financeiro").insert(payload2).execute()
                                inseridas2 += 1
                            else:
                                supabase.table("obras_financeiro").update(payload2).eq("cod4", cod4).execute()
                                atualizadas2 += 1
                        _inline_cache_clear()
                        st.success(f"🎉 {inseridas2 + atualizadas2} registros processados")
                        st.info(f"✅ {criadas2} obras novas  | {inseridas2} inseridas  | {atualizadas2} atualizadas")
                        if sem_cod2:
                            st.warning(f"⚠️ {sem_cod2} linha(s) sem código válido ignoradas")

                    # ══ ROTA 3 — MEDIÇÕES ═════════════════════════════════════
                    elif rota == "3":
                        arquivo.seek(0)
                        df3 = pd.read_csv(arquivo, sep=None, engine="python",
                                          encoding="utf-8-sig", dtype=str, header=0)
                        nomes3 = [
                            "codigo_obra_original","titulo","etapa_obra",
                            "data_emissao","nome_pagador","cnpj_pagador",
                            "numero_nf","numero_nf_remessa","data_vencimento",
                            "descricao","valor","cnpj_recebedor",
                            "razao_social_recebedor","tipo","observacoes","categoria",
                        ]
                        df3 = df3.rename(columns={
                            df3.columns[i]: nomes3[i]
                            for i in range(min(len(df3.columns), len(nomes3)))
                        })

                        # Remove canceladas
                        mask_cancel3 = (df3["tipo"].fillna("").str.strip().str.upper()
                                        == "NOTA FISCAL CANCELADA")
                        n_cancel3 = int(mask_cancel3.sum())
                        df3 = df3[~mask_cancel3].copy()

                        # Converte datas
                        df3["data_emissao"]    = formatar_data(df3["data_emissao"])
                        df3["data_vencimento"] = formatar_data(df3["data_vencimento"])

                        # Converte valor — suporta "R$ 189.916,51" e "189916.51"
                        df3["valor"] = df3["valor"].apply(parse_brl)

                        # Vincula obra
                        mob3 = mapa_obras()
                        df3["obra_id"] = aplicar_obra_id(
                            df3["titulo"].apply(extrair_codigo), mob3)
                        sem_obra3 = int(df3["obra_id"].isna().sum())

                        # Separa registros COM e SEM numero_nf
                        df3_com_nf = df3[df3["numero_nf"].notna() & (df3["numero_nf"].str.strip() != "")].copy()
                        df3_sem_nf = df3[df3["numero_nf"].isna()  | (df3["numero_nf"].str.strip() == "")].copy()

                        cols_bd3 = [
                            "obra_id","codigo_obra_original","titulo","etapa_obra",
                            "data_emissao","nome_pagador","cnpj_pagador","numero_nf",
                            "numero_nf_remessa","data_vencimento","descricao","valor",
                            "cnpj_recebedor","razao_social_recebedor","tipo",
                            "observacoes","categoria",
                        ]
                        cols_bd3 = [c for c in cols_bd3 if c in df3.columns]

                        total3_nf = total3_sem_nf = 0

                        # Registros COM numero_nf: upsert normal
                        if not df3_com_nf.empty:
                            df3_com_nf = df3_com_nf.drop_duplicates(
                                subset=["numero_nf","tipo"], keep="last")
                            pacote3_nf = df3_com_nf[cols_bd3].to_dict("records")
                            pacote3_nf = fix_ids(pacote3_nf)
                            pacote3_nf = limpar_nan_pacote(pacote3_nf)
                            total3_nf = enviar_lotes(
                                "medicoes", pacote3_nf, "Enviando medições com NF...",
                                on_conflict="numero_nf,tipo")

                        # Registros SEM numero_nf: insert simples (sem upsert)
                        if not df3_sem_nf.empty:
                            _cols_sem_nf = [c for c in cols_bd3 if c != "numero_nf"]
                            pacote3_sem = df3_sem_nf[_cols_sem_nf].to_dict("records")
                            pacote3_sem = fix_ids(pacote3_sem)
                            pacote3_sem = limpar_nan_pacote(pacote3_sem)
                            total3_sem_nf = enviar_lotes(
                                "medicoes", pacote3_sem, "Enviando medições sem NF...")

                        total3 = total3_nf + total3_sem_nf
                        _inline_cache_clear()
                        st.success(f"🎉 {total3} medições importadas!")
                        st.info(
                            f"✅ {total3 - sem_obra3} com obra  "
                            f"| ⚠️ {sem_obra3} sem obra  "
                            f"| 📋 {total3_nf} com NF  "
                            f"| 📌 {total3_sem_nf} sem NF  "
                            f"| 🚫 {n_cancel3} canceladas ignoradas")

                    # ══ ROTAS 4 / 5 / 6 — PRODUÇÃO ═══════════════════════════
                    elif rota in ["4","5","6"]:
                        arquivo.seek(0)
                        df_p = pd.read_csv(arquivo, sep=None, engine="python",
                                           encoding="utf-8-sig", dtype=str, header=0)
                        if rota == "4":
                            _np_nomes = [
                                "peca","codigo","obra_codigo","etapa","produto",
                                "secao","qtde_pecas","volume_total","data_fabricacao",
                                "volume_teorico","peso_aco","peso_aco_frouxo",
                                "peso_aco_protendido","comprimento",
                            ]
                            tabela_p  = "producao_fabricacao"
                            num_p     = ["qtde_pecas","volume_total","volume_teorico",
                                         "peso_aco","peso_aco_frouxo",
                                         "peso_aco_protendido","comprimento"]
                            date_p    = "data_fabricacao"
                            cols_bd_p = ["obra_id","cod4_obra","peca","codigo","etapa","produto",
                                         "secao","qtde_pecas","volume_total","data_fabricacao",
                                         "volume_teorico","peso_aco","peso_aco_frouxo",
                                         "peso_aco_protendido","comprimento"]
                        elif rota == "5":
                            _np_nomes = [
                                "peca","codigo","obra_codigo","etapa","produto",
                                "data_expedicao","volume_real","status","peso",
                                "numero_carga","transportadora","motorista","nota_fiscal",
                            ]
                            tabela_p  = "producao_transporte"
                            num_p     = ["volume_real","peso","numero_carga"]
                            date_p    = "data_expedicao"
                            cols_bd_p = ["obra_id","cod4_obra","peca","codigo","etapa","produto",
                                         "data_expedicao","volume_real","status","peso",
                                         "numero_carga","transportadora","motorista","nota_fiscal"]
                        else:
                            _np_nomes = [
                                "peca","codigo","obra_codigo","etapa","produto",
                                "secao","qtde_pecas","volume_total","data_montagem",
                                "volume_teorico","peso",
                            ]
                            tabela_p  = "producao_montagem"
                            num_p     = ["qtde_pecas","volume_total","volume_teorico","peso"]
                            date_p    = "data_montagem"
                            cols_bd_p = ["obra_id","cod4_obra","peca","codigo","etapa","produto",
                                         "secao","qtde_pecas","volume_total","data_montagem",
                                         "volume_teorico","peso"]
                        _np_n = min(len(_np_nomes), len(df_p.columns))
                        df_p.columns = _np_nomes[:_np_n] + list(df_p.columns[_np_n:])
                        mob_p  = mapa_obras()
                        _sel_p = st.session_state.get("imp_col_obra_sel", "🔍 Automático")
                        _man_p = re.sub(r"\D", "", st.session_state.get("imp_cod4_manual", ""))[:4]

                        def _set_prod_obra(df_src, col_ref):
                            df_src["obra_id"]   = aplicar_obra_id(
                                df_src[col_ref].apply(extrair_codigo), mob_p)
                            df_src["cod4_obra"] = df_src[col_ref].apply(extrair_codigo).apply(
                                lambda v: None if (
                                    v is None or str(v).strip().lower()
                                    in ("nan","none","")) else str(v))
                            return df_src

                        if _man_p and len(_man_p) == 4:
                            _oid_p = mob_p.get(_man_p)
                            df_p["obra_id"]   = int(_oid_p) if _oid_p else None
                            df_p["cod4_obra"] = _man_p
                        elif (_sel_p not in ("🔍 Automático", "⛔ Nenhuma (sem vínculo com obra)")
                              and _sel_p in df_p.columns):
                            df_p = _set_prod_obra(df_p, _sel_p)
                        elif _sel_p == "⛔ Nenhuma (sem vínculo com obra)":
                            df_p["obra_id"]   = None
                            df_p["cod4_obra"] = None
                        else:
                            df_p = _set_prod_obra(df_p, "obra_codigo")

                        sem_obra_p = int(pd.Series(df_p["obra_id"]).isna().sum())
                        df_p[date_p] = formatar_data(df_p[date_p])
                        for c_p in num_p:
                            if c_p in df_p.columns:
                                df_p[c_p] = df_p[c_p].apply(parse_brl)
                        df_p = nulos(df_p)
                        # Manter só colunas que existem
                        cols_bd_p_ok = [c for c in cols_bd_p if c in df_p.columns]
                        pacote_p = df_p[cols_bd_p_ok].to_dict("records")
                        pacote_p = fix_ids(pacote_p)
                        pacote_p = limpar_nan_pacote(pacote_p)
                        # Deduplica por codigo (mesmo CSV pode ter duplicatas)
                        _seen_cod = {}
                        for _r in pacote_p:
                            _k = _r.get("codigo")
                            if _k is not None:
                                _seen_cod[_k] = _r
                        pacote_p = list(_seen_cod.values())
                        total_p = enviar_lotes(tabela_p, pacote_p,
                                               f"Enviando para {tabela_p}...",
                                               on_conflict="codigo")
                        _inline_cache_clear()
                        st.success(f"🎉 {total_p} registros atualizados em {tabela_p}!")
                        st.info(
                            f"✅ {total_p - sem_obra_p} com obra vinculada"
                            + (f" | ⚠️ {sem_obra_p} sem obra (obra_id NULL)"
                               if sem_obra_p else ""))

                    # ══ ROTA 7 — CUSTOS ═══════════════════════════════════════
                    elif rota == "7":
                        arquivo.seek(0)
                        df7 = pd.read_excel(arquivo, engine="openpyxl", dtype=str)
                        # Mapeamento exato de colunas (nomes do Export.csv do ERP)
                        col_map = {
                            # nomes exatos do Export.csv do ERP
                            "Data":                        "data",
                            "data":                        "data",
                            "IdLanç":                      "id_lancamento",
                            "id_lancamento":               "id_lancamento",
                            "NºDoc":                       "numero_doc",
                            "numero_doc":                  "numero_doc",
                            "Centro de Custos":            "centro_custos",
                            "centro_custos":               "centro_custos",
                            "Conta Macro":                 "conta_macro",
                            "conta_macro":                 "conta_macro",
                            "Conta Gerencial":             "conta_gerencial",
                            "conta_gerencial":             "conta_gerencial",
                            "Cli/For":                     "cli_fornecedor",
                            "cli_fornecedor":              "cli_fornecedor",
                            "fornecedor":                  "cli_fornecedor",
                            "Produto_Serviço":             "produto_servico",
                            "Produto_Servico":             "produto_servico",
                            "produto_servico":             "produto_servico",
                            "CriadoPor":                   "criado_por",
                            "criado_por":                  "criado_por",
                            "usuario":                     "criado_por",
                            " Valor Global ":              "valor_global",
                            "Valor Global":                "valor_global",
                            "valor_global":                "valor_global",
                            "valor":                       "valor_global",
                            "Qtd":                         "qtd",
                            "qtd":                         "qtd",
                            "quantidade":                  "qtd",
                            " PrecoUnitario ":             "preco_unitario",
                            "PrecoUnitario":               "preco_unitario",
                            "preco_unitario":              "preco_unitario",
                            "Origem":                      "origem",
                            "origem":                      "origem",
                            "Chave_ColigadaIdOrigem":      "chave_coligada",
                            "chave_coligada":              "chave_coligada",
                            "Cód. e Tipo Doc/Movimento":   "cod_tipo_doc",
                            "cod_tipo_doc":                "cod_tipo_doc",
                        }
                        # Strip de espaços nos nomes de colunas antes do rename
                        df7.columns = [c.strip() for c in df7.columns]
                        df7 = df7.rename(columns=col_map)
                        # Correlação informativa (custos não tem obra_id no BD)
                        _sel7 = st.session_state.get("imp_col_obra_sel", "🔍 Automático")
                        _man7 = re.sub(r"\D", "", st.session_state.get("imp_cod4_manual", ""))[:4]

                        if _man7 and len(_man7) == 4:
                            _ref_col7 = None
                            _ref_cod4_fixo7 = _man7
                        elif (_sel7 not in ("🔍 Automático", "⛔ Nenhuma (sem vínculo com obra)")
                              and _sel7 in df7.columns):
                            _ref_col7 = _sel7
                            _ref_cod4_fixo7 = None
                        elif _sel7 == "⛔ Nenhuma (sem vínculo com obra)":
                            _ref_col7 = None
                            _ref_cod4_fixo7 = None
                        else:
                            _ref_cod4_fixo7 = None
                            _ref_col7 = next(
                                (c for c in ["obra_codigo","obra","centro_custos"]
                                 if c in df7.columns
                                 and df7[c].apply(extrair_codigo).notna().any()),
                                None)

                        if _ref_cod4_fixo7:
                            com_obra7, sem_obra7 = len(df7), 0
                        elif _ref_col7:
                            _s7 = df7[_ref_col7].apply(extrair_codigo)
                            com_obra7 = int(_s7.notna().sum())
                            sem_obra7 = int(_s7.isna().sum())
                        else:
                            com_obra7, sem_obra7 = 0, len(df7)

                        df7 = df7.replace("", None).where(pd.notnull(df7), None)
                        if "data" in df7.columns:
                            df7["data"] = formatar_data(df7["data"])
                        for col7 in ["valor_global","qtd","preco_unitario"]:
                            if col7 in df7.columns:
                                df7[col7] = df7[col7].apply(parse_brl)
                        # Remove linha de rodapé nula (totalizador do arquivo ERP)
                        df7 = df7.dropna(subset=["data"]).copy()
                        df7 = df7[df7["valor_global"].notna()].copy()
                        # Extrai cod4 do centro_custos
                        if "centro_custos" in df7.columns:
                            df7["cod4"] = df7["centro_custos"].apply(extrair_codigo)
                        cols_bd7 = ["data","id_lancamento","numero_doc",
                                    "centro_custos","conta_macro","conta_gerencial",
                                    "cli_fornecedor","produto_servico","criado_por",
                                    "valor_global","qtd","preco_unitario","origem",
                                    "chave_coligada","cod_tipo_doc","cod4"]
                        cols_bd7_ok = [c for c in cols_bd7 if c in df7.columns]
                        pacote7_raw = df7[cols_bd7_ok].to_dict("records")
                        pacote7 = limpar_nan_pacote(pacote7_raw)
                        # Deduplica pela combinação que identifica um lançamento único
                        _seen7 = {}
                        for _r7 in pacote7:
                            _k7 = (
                                str(_r7.get("data") or ""),
                                str(_r7.get("centro_custos") or ""),
                                str(_r7.get("conta_gerencial") or ""),
                                str(_r7.get("cli_fornecedor") or ""),
                                str(_r7.get("valor_global") or ""),
                            )
                            _seen7[_k7] = _r7
                        _n_dedup7 = len(pacote7) - len(_seen7)
                        pacote7 = list(_seen7.values())
                        _sub7 = st.checkbox(
                            "🗑️ Substituir tudo (apaga registros existentes antes de inserir)",
                            key="imp_sub7",
                            help="Use ao reimportar para evitar duplicatas.")
                        total7 = enviar_lotes("custos", pacote7, "Enviando custos...",
                                              truncate_before=_sub7)
                        _inline_cache_clear()
                        try:
                            supabase.rpc("refresh_all").execute()
                        except Exception:
                            pass
                        st.success(f"🎉 {total7} lançamentos importados!")
                        st.info(f"✅ {com_obra7} diretos (com obra) | 🏭 {sem_obra7} indiretos")
                        if _n_dedup7 > 0:
                            st.warning(f"⚠️ {_n_dedup7} lançamentos duplicados removidos antes de importar")

                    # ══ ROTAS 9 / 10 — RECEITAS / DESPESAS (RM) ══════════════
                    elif rota in ("9", "10"):
                        tabela_rm = "receitas" if rota == "9" else "despesas"
                        label_rm  = "Receitas" if rota == "9" else "Despesas"

                        try:
                            arquivo.seek(0)
                            df_rm = pd.read_excel(arquivo, engine="openpyxl", dtype=str)
                        except Exception as e:
                            st.error(f"❌ Erro ao ler XLSX: {e}")
                            st.stop()

                        # Strip de espaços nos nomes de colunas
                        df_rm.columns = [c.strip() for c in df_rm.columns]

                        # Mapeamento de colunas
                        col_map_rm = {
                            "Data":                       "data",
                            "IdLanç":                     "id_lancamento",
                            "NºDoc":                      "numero_doc",
                            "Centro de Custos":           "centro_custos",
                            "Conta Macro":                "conta_macro",
                            "Conta Gerencial":            "conta_gerencial",
                            "Cli/For":                    "cli_fornecedor",
                            "Produto_Serviço":            "produto_servico",
                            "Produto_Servico":            "produto_servico",
                            "CriadoPor":                  "criado_por",
                            " Valor Global ":             "valor_global",
                            "Valor Global":               "valor_global",
                            "Qtd":                        "qtd",
                            " PrecoUnitario ":            "preco_unitario",
                            "PrecoUnitario":              "preco_unitario",
                            "Origem":                     "origem",
                            "Chave_ColigadaIdOrigem":     "chave_coligada",
                            "Cód. e Tipo Doc/Movimento":  "cod_tipo_doc",
                        }
                        df_rm = df_rm.rename(columns=col_map_rm)

                        # Extrai cod4 do centro_custos
                        if "centro_custos" in df_rm.columns:
                            df_rm["cod4"] = df_rm["centro_custos"].apply(extrair_codigo)
                            com_obra_rm = int(df_rm["cod4"].notna().sum())
                            sem_obra_rm = int(df_rm["cod4"].isna().sum())
                        else:
                            com_obra_rm, sem_obra_rm = 0, len(df_rm)

                        # Formata datas e numéricos
                        df_rm = df_rm.replace("", None).where(pd.notnull(df_rm), None)
                        if "data" in df_rm.columns:
                            df_rm["data"] = formatar_data(df_rm["data"])
                        for col_rm in ["valor_global", "qtd", "preco_unitario"]:
                            if col_rm in df_rm.columns:
                                df_rm[col_rm] = df_rm[col_rm].apply(parse_brl)
                        # Remove linha de rodapé nula (totalizador do arquivo ERP)
                        df_rm = df_rm.dropna(subset=["data"]).copy()
                        df_rm = df_rm[df_rm["valor_global"].notna()].copy()

                        cols_bd_rm = ["data","id_lancamento","numero_doc",
                                      "centro_custos","conta_macro","conta_gerencial",
                                      "cli_fornecedor","produto_servico","criado_por",
                                      "valor_global","qtd","preco_unitario","origem",
                                      "chave_coligada","cod_tipo_doc","cod4"]
                        cols_bd_rm_ok = [c for c in cols_bd_rm if c in df_rm.columns]
                        pacote_rm_raw = df_rm[cols_bd_rm_ok].to_dict("records")
                        pacote_rm = limpar_nan_pacote(pacote_rm_raw)
                        # Deduplica pela combinação que identifica um lançamento único
                        _seen_rm = {}
                        for _r_rm in pacote_rm:
                            _k_rm = (
                                str(_r_rm.get("data") or ""),
                                str(_r_rm.get("centro_custos") or ""),
                                str(_r_rm.get("conta_gerencial") or ""),
                                str(_r_rm.get("cli_fornecedor") or ""),
                                str(_r_rm.get("valor_global") or ""),
                            )
                            _seen_rm[_k_rm] = _r_rm
                        _n_dedup_rm = len(pacote_rm) - len(_seen_rm)
                        pacote_rm = list(_seen_rm.values())

                        total_rm = enviar_lotes(tabela_rm, pacote_rm, f"Enviando {label_rm}...")
                        _inline_cache_clear()
                        try:
                            supabase.rpc("refresh_all").execute()
                        except Exception:
                            pass
                        st.success(f"🎉 {total_rm} lançamentos de {label_rm} importados!")
                        st.info(f"✅ {com_obra_rm} com cod4 (vinculados) | 🏭 {sem_obra_rm} sem cod4")
                        if _n_dedup_rm > 0:
                            st.warning(f"⚠️ {_n_dedup_rm} lançamentos duplicados removidos antes de importar")

                    # ══ ROTA 8 — FOLHA / RH ═══════════════════════════════════
                    elif rota == "8":
                        # Recupera mês selecionado fora do botão
                        mes_comp = st.session_state.get("folha_mes_comp")
                        if not mes_comp:
                            st.warning("⚠️ Selecione o mês de competência antes de importar.")
                            st.stop()

                        # Lê o arquivo como xlsx
                        try:
                            arquivo.seek(0)
                            df8 = pd.read_excel(arquivo, engine="openpyxl", dtype=str)
                        except Exception as e:
                            st.error(f"❌ Erro ao ler XLSX: {e}")
                            st.stop()

                        # Remove linhas completamente vazias ou sem nome
                        df8 = df8.dropna(subset=["Nome do Colaborador"]).copy()
                        df8 = df8[df8["Nome do Colaborador"].str.strip() != ""].copy()

                        # Mapeamento de colunas xlsx → banco
                        col_map8 = {
                            "Nome do Colaborador":        "nome_colaborador",
                            "Função":                     "funcao",
                            "CPF":                        "cpf",
                            "Situação":                   "situacao",
                            "Proventos":                  "proventos",
                            "Base FGTS + Base FGTS 13º": "base_fgts",
                            "Base INSS + Base INSS 13º":  "base_inss",
                            "h.e 50%":                    "he_50",
                            "h.e 70%":                    "he_70",
                            "h.e 80%":                    "he_80",
                            "h.e 100%":                   "he_100",
                            "h.e 110%":                   "he_110",
                            "h.e 150%":                   "he_150",
                            "Adc. Noturno":               "adc_noturno",
                            "D.S.R.":                     "dsr",
                            "Desconto Compart":           "desconto_compart",
                            "Vale Transporte":            "vale_transporte",
                            "Alimentação":                "alimentacao",
                            "Seguro de vida":             "seguro_vida",
                            "Assistencia Médica":         "assistencia_medica",
                            "Soma HEs + D.S.R":           "soma_hes",
                            "Proventos 13º":              "proventos_13",
                            "Base FGTS 13":               "base_fgts_13",
                            "Desc. 1ª 13º":               "desc_1_13",
                            "FGTS ARTIGO 22":             "fgts_art22",
                            "Valor do Funcionário":       "valor_funcionario",
                            "Coligada":                   "empresa",
                        }
                        df8 = df8.rename(columns=col_map8)

                        # Colunas numéricas
                        cols_num8 = [
                            "proventos", "base_fgts", "base_inss",
                            "he_50", "he_70", "he_80", "he_100", "he_110", "he_150",
                            "adc_noturno", "dsr", "desconto_compart", "vale_transporte",
                            "alimentacao", "seguro_vida", "assistencia_medica", "soma_hes",
                            "proventos_13", "base_fgts_13", "desc_1_13", "fgts_art22",
                            "valor_funcionario"
                        ]
                        for c in cols_num8:
                            if c in df8.columns:
                                df8[c] = pd.to_numeric(df8[c], errors="coerce")
                                df8[c] = df8[c].apply(
                                    lambda v: None if (v is None or (isinstance(v, float) and (_math.isnan(v) or _math.isinf(v)))) else v
                                )

                        # Colunas de texto
                        for c in ["nome_colaborador", "funcao", "cpf", "situacao", "empresa"]:
                            if c in df8.columns:
                                df8[c] = df8[c].apply(
                                    lambda v: None if (v is None or str(v).strip().lower() in ("nan", "none", "")) else str(v).strip()
                                )

                        # Adiciona mês de competência
                        df8["mes"] = mes_comp.isoformat()

                        # Monta pacote final com apenas colunas existentes no banco
                        cols_bd8 = [
                            "nome_colaborador", "funcao", "cpf", "situacao", "mes", "empresa",
                            "proventos", "base_fgts", "base_inss",
                            "he_50", "he_70", "he_80", "he_100", "he_110", "he_150",
                            "adc_noturno", "dsr", "desconto_compart", "vale_transporte",
                            "alimentacao", "seguro_vida", "assistencia_medica", "soma_hes",
                            "proventos_13", "base_fgts_13", "desc_1_13", "fgts_art22",
                            "valor_funcionario"
                        ]
                        cols_bd8_ok = [c for c in cols_bd8 if c in df8.columns]
                        pacote8 = df8[cols_bd8_ok].to_dict("records")

                        # Limpa NaN residuais
                        pacote8_limpo = []
                        for row8 in pacote8:
                            pacote8_limpo.append({
                                k: None if (
                                    v is None
                                    or (isinstance(v, float) and (_math.isnan(v) or _math.isinf(v)))
                                    or str(v).strip().lower() in ("nan", "nat", "none", "inf", "")
                                ) else v
                                for k, v in row8.items()
                            })

                        total8 = enviar_lotes("folha", pacote8_limpo, "Enviando folha...")
                        _inline_cache_clear()
                        st.success(f"🎉 {total8} colaboradores importados para o mês {mes_comp.strftime('%m/%Y')}!")
                        ativos8  = sum(1 for r in pacote8_limpo if r.get("situacao") == "A")
                        demit8   = sum(1 for r in pacote8_limpo if r.get("situacao") == "D")
                        ferias8  = sum(1 for r in pacote8_limpo if r.get("situacao") == "F")
                        st.info(f"✅ {ativos8} ativos | 🏖️ {ferias8} férias | 🚪 {demit8} desligados")

                except Exception as _e:
                    st.error(f"❌ Erro: {_e}")
                    st.code(_tb.format_exc(), language="python")

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

@st.cache_data(ttl=600)
def carregar_obras_ativas():
    resp = supabase.table("obras")\
        .select("id, cod4, nome, status, modalidade, cliente, responsavel_id")\
        .order("nome").execute()
    return resp.data

@st.cache_data(ttl=600)
def carregar_equipe_ativa():
    resp = supabase.table("equipe").select("id, nome").eq("status","Ativo").order("nome").execute()
    return resp.data

@st.cache_data(ttl=3600)
def carregar_template():
    resp = supabase.table("template_jornada").select("*").order("item").execute()
    return resp.data

@st.cache_data(ttl=120)
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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=600)
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

@st.cache_data(ttl=120)
def carregar_tarefas_colab(colab_id):
    """Busca todas as tarefas onde o colaborador aparece em qualquer papel RACI."""
    resp = supabase.table("obras_tarefas")\
        .select("id, item, etapa, descricao, status, observacoes, impedimento, "
                "avanco_percent, inicio_previsto, entrega_prevista, entrega_real, "
                "gut_gravidade, gut_urgencia, gut_tendencia, gut_score, obra_id, origem, "
                "responsavel_id, aprovador_id, consultado_id, informado_id, "
                "obras(nome, cod4)")\
        .or_(f"responsavel_id.eq.{colab_id},aprovador_id.eq.{colab_id},"
             f"consultado_id.eq.{colab_id},informado_id.eq.{colab_id}")\
        .execute()
    return resp.data

def limpar_todos_caches():
    """Invalida TODOS os caches de uma vez.
    Usar após qualquer importação ou alteração de dados."""
    try:
        st.cache_data.clear()
    except Exception:
        pass

def limpar_cache():
    """Mantido por compatibilidade — delega para limpar_todos_caches()."""
    limpar_todos_caches()

# ── PAINEL LATERAL — EDIÇÃO RÁPIDA DE OBRA ──────────────
_SB_STATUS   = ["Em Andamento", "Concluída", "Cancelada", "Proposta"]
_SB_MODALS   = ["FOB", "CIF", "Montagem", "Não definida"]
_SB_ST_ICON  = {"Em Andamento": "🔵", "Concluída": "🟢", "Cancelada": "🔴", "Proposta": "🟡"}

with st.sidebar.expander("✏️ Editar obra", expanded=False):
    _obras_sb  = carregar_obras_ativas()
    _equipe_sb = carregar_equipe_ativa()

    if not _obras_sb:
        st.caption("Nenhuma obra cadastrada.")
    else:
        _opc_sb   = {f"{o['cod4']} — {o['nome']}": o for o in _obras_sb}
        _sb_lbl   = st.selectbox("Obra", list(_opc_sb.keys()),
                                 key="sb_obra_sel", label_visibility="collapsed")
        _osb      = _opc_sb[_sb_lbl]
        _oid_sb   = _osb["id"]

        _cur_st   = _osb.get("status") or "Em Andamento"
        st.caption(f"{_SB_ST_ICON.get(_cur_st,'⚪')} Status atual: **{_cur_st}**")

        _idx_s    = _SB_STATUS.index(_cur_st) if _cur_st in _SB_STATUS else 0
        _new_st   = st.selectbox("Status", _SB_STATUS, index=_idx_s, key="sb_ed_status")

        _cur_m    = _osb.get("modalidade") or "Não definida"
        _idx_m    = _SB_MODALS.index(_cur_m) if _cur_m in _SB_MODALS else 3
        _new_m    = st.selectbox("Modalidade", _SB_MODALS, index=_idx_m, key="sb_ed_modal")

        _new_cli  = st.text_input("Cliente", value=_osb.get("cliente") or "", key="sb_ed_cliente")

        _rnomes_sb  = [e["nome"] for e in _equipe_sb]
        _rids_sb    = {e["nome"]: e["id"] for e in _equipe_sb}
        _resp_opt   = ["—"] + _rnomes_sb
        _cur_rid    = _osb.get("responsavel_id")
        _cur_rnm    = next((e["nome"] for e in _equipe_sb if e["id"] == _cur_rid), None)
        _idx_r      = _resp_opt.index(_cur_rnm) if _cur_rnm in _resp_opt else 0
        _new_resp   = st.selectbox("Responsável", _resp_opt, index=_idx_r, key="sb_ed_resp")

        if st.button("💾 Salvar", key="btn_sb_salvar_obra", use_container_width=True):
            _novo_rid = _rids_sb.get(_new_resp) if _new_resp != "—" else None
            supabase.table("obras").update({
                "status":         _new_st,
                "modalidade":     _new_m,
                "cliente":        _new_cli.strip() or None,
                "responsavel_id": int(_novo_rid) if _novo_rid else None,
            }).eq("id", _oid_sb).execute()
            limpar_cache()
            st.success("✅ Salvo!")
            st.rerun()
# ────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def volume_referencia(obra_id):
    """Retorna o volume de referência da obra para uso financeiro.
    Prioridade: volume_projeto > volume (comercial) > None."""
    try:
        r = supabase.table("obras").select("cod4").eq("id", obra_id).limit(1).execute()
        if not r.data: return None
        cod4 = r.data[0].get("cod4")
        if not cod4: return None
        resp = supabase.table("obras_financeiro")\
            .select("volume_projeto, volume")\
            .eq("cod4", cod4)\
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
# FUNÇÕES COMPARTILHADAS — TODOS OS DASHBOARDS
# ==========================================================

@st.cache_data(ttl=600)
def carregar_obras_completo():
    """obras LEFT JOIN obras_financeiro — base de todos os dashboards."""
    resp_o = supabase.table("obras")\
        .select("id, cod4, nome, status, modalidade, cliente")\
        .order("nome").execute()
    df_o = pd.DataFrame(resp_o.data or [])
    if df_o.empty:
        return df_o
    cols_fin = ("cod4, faturamento_total, volume, volume_projeto, custo_total, lucro,"
                " concreto, aco_estrutural, formas, mo_producao, materiais_consumo,"
                " equip_fab, custos_indiretos, eps, estuque, insertos, consoles, neoprene,"
                " descida_agua, pecas_consorcio, investimentos, frete, equip_montagem,"
                " mo_montagem, despesas_equipe, topografia, mobilizacao, equip_aux_montagem,"
                " outros, eventuais, despesas_comerciais")
    resp_f = supabase.table("obras_financeiro").select(cols_fin).execute()
    df_f   = pd.DataFrame(resp_f.data or [])
    if not df_f.empty:
        df = df_o.merge(df_f, on="cod4", how="left")
    else:
        df = df_o.copy()
    num_cols = [
        "faturamento_total","volume","volume_projeto","custo_total","lucro",
        "concreto","aco_estrutural","formas","mo_producao","materiais_consumo",
        "equip_fab","custos_indiretos","eps","estuque","insertos","consoles",
        "neoprene","descida_agua","pecas_consorcio","investimentos","frete",
        "equip_montagem","mo_montagem","despesas_equipe","topografia",
        "mobilizacao","equip_aux_montagem","outros","eventuais","despesas_comerciais",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

@st.cache_data(ttl=900)
def carregar_medicoes_resumo():
    """medicoes sem canceladas/remessas, com campo mes."""
    df = fetch_all("medicoes", "obra_id, data_emissao, descricao, tipo, valor")
    if df.empty:
        return df
    excluir = {"NOTA FISCAL CANCELADA", "NOTA FISCAL REMESSA"}
    df = df[~df["tipo"].str.strip().str.upper().isin(excluir)].copy()
    df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
    df["valor"]        = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
    df["mes"]          = df["data_emissao"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=300)
def carregar_producao_resumo():
    """Fabricação agregada por obra/mês — lê da view mv_fabricacao_mensal."""
    df = pd.DataFrame(
        supabase.table("mv_fabricacao_mensal").select("*").execute().data or []
    )
    if df.empty:
        return df
    for col in ["vol_teorico","vol_total","peso_aco","peso_frouxo","peso_protendido","pecas"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["mes"] = pd.to_datetime(df["mes"], errors="coerce").dt.to_period("M").astype(str)
    df = df.rename(columns={
        "vol_teorico":    "volume_teorico",
        "vol_total":      "volume_total",
        "peso_frouxo":    "peso_aco_frouxo",
        "peso_protendido":"peso_aco_protendido",
    })
    return df

@st.cache_data(ttl=300)
def carregar_transporte_resumo():
    """Transporte agregado por obra/mês — lê da view mv_transporte_mensal."""
    df = pd.DataFrame(
        supabase.table("mv_transporte_mensal").select("*").execute().data or []
    )
    if df.empty:
        return df
    for col in ["vol_real","peso_total","expedicoes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["mes"] = pd.to_datetime(df["mes"], errors="coerce").dt.to_period("M").astype(str)
    df = df.rename(columns={"vol_real": "volume_real"})
    return df

@st.cache_data(ttl=300)
def carregar_montagem_resumo():
    """Montagem agregada por obra/mês — lê da view mv_montagem_mensal."""
    df = pd.DataFrame(
        supabase.table("mv_montagem_mensal").select("*").execute().data or []
    )
    if df.empty:
        return df
    for col in ["vol_montado","montagens"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["mes"] = pd.to_datetime(df["mes"], errors="coerce").dt.to_period("M").astype(str)
    df = df.rename(columns={"vol_montado": "volume_teorico"})
    return df

@st.cache_data(ttl=300)
def carregar_patio_resumo():
    """Situação do pátio por obra — lê da view mv_patio_atual."""
    df = pd.DataFrame(
        supabase.table("mv_patio_atual").select("*").execute().data or []
    )
    if df.empty:
        return df
    for col in ["pecas_no_patio","vol_no_patio","prazo_medio_dias"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

@st.cache_data(ttl=300)
def carregar_custos_resumo():
    """Custos + despesas para eficiência — lê da view mv_custos_completo."""
    df = pd.DataFrame(
        supabase.table("mv_custos_completo").select(
            "data, conta_macro, conta_gerencial, centro_custos, cli_fornecedor, valor_global, valor_abs"
        ).execute().data or []
    )
    if df.empty:
        return df
    df["data"]         = pd.to_datetime(df["data"], errors="coerce")
    df["valor_global"] = pd.to_numeric(df["valor_global"], errors="coerce").abs().fillna(0)
    df["mes"]          = df["data"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=300)
def carregar_tarefas_extras():
    """Tarefas extras — obra_id IS NULL (não vinculadas a obra)."""
    resp = supabase.table("obras_tarefas")\
        .select("id, item, etapa, descricao, status, observacoes, impedimento, origem,"
                " avanco_percent, inicio_previsto, entrega_prevista, entrega_real,"
                " gut_gravidade, gut_urgencia, gut_tendencia, gut_score,"
                " responsavel_id, aprovador_id,"
                " responsavel:equipe!obras_tarefas_responsavel_id_fkey(nome)")\
        .is_("obra_id", "null")\
        .execute()
    return resp.data

def calcular_custos_categorias_g(row):
    """Breakdown de custo previsto: fabricacao / transporte / montagem."""
    def v(col):
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return 0.0
        return float(val)
    return {
        "fabricacao": sum(v(c) for c in [
            "concreto","aco_estrutural","formas","mo_producao","materiais_consumo",
            "equip_fab","custos_indiretos","eps","estuque","insertos","consoles",
            "neoprene","descida_agua","pecas_consorcio","investimentos"]),
        "transporte": v("frete"),
        "montagem":   sum(v(c) for c in [
            "mo_montagem","equip_montagem","equip_aux_montagem",
            "despesas_equipe","topografia","mobilizacao"]),
    }

def volume_ref(row):
    """Volume de referência da obra: volume_projeto > volume > 0."""
    for col in ("volume_projeto", "volume"):
        val = row.get(col)
        if val is not None:
            try:
                f = float(val)
                if f > 0:
                    return f
            except Exception:
                pass
    return 0.0

def fmt_brl(valor):
    """Formata float como moeda brasileira: R$ 1.234.567"""
    if valor is None:
        return "—"
    try:
        v = float(valor)
        if pd.isna(v):
            return "—"
    except Exception:
        return "—"
    sinal = "-" if v < 0 else ""
    return (f"{sinal}R$ "
            + f"{abs(v):,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))

def fetch_all(tabela, colunas="*", page_size=5000, **filtros_eq):
    """Carrega todos os registros com paginação automática.
    filtros_eq: pares coluna=valor para filtros de igualdade simples.
    Exemplo: fetch_all('custos', 'data,valor_global', obra_id=42)
    """
    rows, page = [], 0
    q = supabase.table(tabela).select(colunas)
    for col, val in filtros_eq.items():
        q = q.eq(col, val)
    while True:
        batch = q.range(page * page_size, (page + 1) * page_size - 1).execute().data
        rows.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return pd.DataFrame(rows)

def parse_brl(valor):
    """Converte qualquer representação de número para float.
    Suporta: 'R$ 1.234,56', '1234.56', '-1.234', '1,234.56'
    Retorna None para vazios, NaN, infinitos.
    """
    import math as _m, re as _re
    if valor is None:
        return None
    s = _re.sub(r"[^\d,.\-]", "", str(valor).strip())
    if not s or s in ("-", "."):
        return None
    last_dot   = s.rfind(".")
    last_comma = s.rfind(",")
    if last_dot > 0 and last_comma > 0:
        if last_comma > last_dot:          # 1.234,56 → brasileiro
            s = s.replace(".", "").replace(",", ".")
        else:                              # 1,234.56 → americano
            s = s.replace(",", "")
    elif last_comma > 0:                   # só vírgula → 1234,56
        s = s.replace(",", ".")
    try:
        v = float(s)
        if _m.isnan(v) or _m.isinf(v) or abs(v) >= 1e13:
            return None
        return v
    except Exception:
        return None

# ── Mapeamento global conta_gerencial → grupo_custo ────────
# Usado por carregar_custos_completo e _enriquecer_df (Custos page)
_MAP_GRUPO_GLOBAL = {
    "CIMENTO":                              "Insumos Estruturais",
    "AÇO PARA ESTRUTURAS":                  "Insumos Estruturais",
    "CORDOALHA":                            "Insumos Estruturais",
    "AREIA NATURAL":                        "Insumos Estruturais",
    "PEDRAS BRITADAS":                      "Insumos Estruturais",
    "ADITIVO":                              "Insumos Estruturais",
    "METACAULIM":                           "Insumos Estruturais",
    "MINÉRIO NÃO REAGIDO":                  "Insumos Estruturais",
    "MATERIAIS DE CONSUMO":                 "Materiais de Consumo",
    "PEÇAS E ACESSÓRIOS":                   "Peças e Manutenção",
    "FERRAMENTAS E UTENSÍLIOS":             "Peças e Manutenção",
    "PEÇAS DE DESGASTE":                    "Peças e Manutenção",
    "GASES E ELETRODOS":                    "Peças e Manutenção",
    "LUBRIFICANTES":                        "Peças e Manutenção",
    "PNEUS E CÂMARAS":                      "Peças e Manutenção",
    "SERVIÇOS DE MANUTENÇÃO DE EQUIP. PJ":  "Peças e Manutenção",
    "SALARIOS":                             "Pessoal",
    "INSS S/ FOLHA":                        "Pessoal",
    "INSS FOLHA":                           "Pessoal",
    "FGTS":                                 "Pessoal",
    "HORAS EXTRAS":                         "Pessoal",
    "FERIAS":                               "Pessoal",
    "DECIMO TERCEIRO  SALARIO":             "Pessoal",
    "ADICIONAL NOTURNO":                    "Pessoal",
    "ADICIONAL DE PERICULOSIDADE":          "Pessoal",
    "13 SALARIO INDENIZADO":                "Pessoal",
    "FERIAS INDENIZADAS":                   "Pessoal",
    "AVISO PREVIO INDENIZADO":              "Pessoal",
    "FGTS RESCISÃO":                        "Pessoal",
    "PRODUTIVIDADE":                        "Pessoal",
    "1/3 FERIAS":                           "Pessoal",
    "ABONO PECUNIARIO DE FERIAS":           "Pessoal",
    "1/3 ABONO PECUNIARIO DE FERIAS":       "Pessoal",
    "ANUENIO":                              "Pessoal",
    "MULTA ART 480":                        "Pessoal",
    "ALIMENTAÇÃO":                          "Benefícios",
    "ALIMENTACAO":                          "Benefícios",
    "ASSISTÊNCIA MÉDICA":                   "Benefícios",
    "ASSISTENCIA MEDICA/ODONTOLOGICA":      "Benefícios",
    "SEGURO DE VIDA":                       "Benefícios",
    "VALE-TRANSPORTE":                      "Benefícios",
    "TRANSPORTE DE FUNCIONARIOS":           "Benefícios",
    "CESTAS BASICAS":                       "Benefícios",
    "EXAMES OCUPACIONAIS":                  "Benefícios",
    "FARDAMENTO E EQUIPAMENTO DE SEGURANÇA": "Benefícios",
    "ESTAGIARIOS":                          "Benefícios",
    "OUTROS SERVIÇOS PJ":                   "Serviços PJ",
    "SERVIÇOS PJ PARA PRODUÇÃO":            "Serviços PJ",
    "LOCAÇÕES PJ":                          "Serviços PJ",
    "SERVIÇOS TÉCNICOS":                    "Serviços PJ",
    "SERVIÇOS DE CONSULTORIAS PJ":          "Serviços PJ",
    "SERVIÇOS DE DESENV./SUPORTE DE SISTEMAS": "Serviços PJ",
    "SERVIÇOS DE CONSERVAÇÃO E LIMPEZA PJ": "Serviços PJ",
    "FRETE PJ":                             "Logística",
    "FRETE PJ PARA CLIENTES":              "Logística",
    "COMBUSTÍVEIS":                         "Logística",
    "PEDÁGIOS/ESTACIONAMENTOS/CONDUÇÕES":   "Logística",
    "ENERGIA ELÉTRICA":                     "Energia e Infraestrutura",
    "DEPRECIAÇÕES":                         "Energia e Infraestrutura",
    "DEPRECIACOES":                         "Energia e Infraestrutura",
    "MATERIAIS APLICADOS EM REFORMAS":      "Energia e Infraestrutura",
}

@st.cache_data(ttl=300)
def carregar_custos_completo():
    """Custos + despesas unificados — lê da view mv_custos_completo."""
    df = pd.DataFrame(
        supabase.table("mv_custos_completo").select("*").execute().data or []
    )
    if df.empty:
        return df
    df["data"]         = pd.to_datetime(df["data"], errors="coerce")
    df["valor_global"] = pd.to_numeric(df["valor_global"], errors="coerce").fillna(0)
    df["valor_abs"]    = df["valor_global"].abs()
    df = df[df["data"].notna()].copy()
    df["mes"]          = df["data"].dt.to_period("M").astype(str)
    df["ano"]          = df["data"].dt.year.astype(str)
    df["grupo_custo"]  = (
        df["conta_gerencial"]
        .fillna("").str.upper().str.strip()
        .map(_MAP_GRUPO_GLOBAL)
        .fillna("Outros")
    )
    _kw_equip = ["CAMINHÃO","CAMINHAO","PÓRTICO","PORTICO",
                 "RETROESCAVADEIRA","BETONEIRA","MUNCK","MAQUINA","MÁQUINA"]
    _kw_indir = ["PRODUÇÃO","PRODUCAO","LABORATÓRIO","LABORATORIO",
                 "MONTAGEM","MANUTENÇÃO","MANUTENCAO"]
    _cc = df["centro_custos"].fillna("").str.upper()
    df["tipo_centro"] = np.where(
        df["vinculo"] == "Com Obra", "Direto (Obra)",
        np.where(
            _cc.apply(lambda x: any(k in x for k in _kw_equip)),
            "Equipamento",
            np.where(
                _cc.apply(lambda x: any(k in x for k in _kw_indir)),
                "Indireto (Fábrica)",
                "Outros"
            )
        )
    )
    return df

# ==========================================================
# PÁGINA: GESTÃO DE OBRAS (SÚMULA)
# ==========================================================
if pagina_selecionada == "🏗️ Gestão de Obras":
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
    _status_opts_go = ["Todos"] + status_disponiveis
    _default_go = _status_opts_go.index("Em Andamento") if "Em Andamento" in _status_opts_go else 0
    filtro_status_obra = r1.selectbox("Status", _status_opts_go,
        index=_default_go,
        key="filt_obra_status", label_visibility="collapsed")

    obras_filtradas = obras if filtro_status_obra == "Todos" \
        else [o for o in obras if o.get("status") == filtro_status_obra]
    if not obras_filtradas: st.warning("Nenhuma obra."); st.stop()

    ultimo_upd = carregar_ultimo_update()

    def label_obra(o):
        upd_str = ultimo_upd.get(o["id"])
        return f"{o['cod4']} — {o['nome']}" + (f" · 🕐{upd_str}" if upd_str else "")

    opcoes_obras = {label_obra(o): o["id"] for o in obras_filtradas}
    obra_label   = r2.selectbox("Obra", list(opcoes_obras.keys()),
        key="sel_obra", label_visibility="collapsed")
    obra_id = opcoes_obras[obra_label]

    if r3.button("🔄 Atualizar"):
        limpar_cache(); st.rerun()

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

    # ── ABAS PRINCIPAIS ──────────────────────────────────
    aba_sumula, aba_extras, aba_dash = st.tabs(["🏗️ Obras", "📌 Extras", "📊 Dashboard"])

    with aba_sumula:
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
        else:
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

    # ════════════════════════════════════════════════════════
    # ABA EXTRAS — tarefas sem obra (obra_id IS NULL)
    # ════════════════════════════════════════════════════════
    with aba_extras:
        extras_lista = carregar_tarefas_extras()
        _hoje_e = date.today()

        ef1, ef2, ef3 = st.columns([2,2,1])
        f_ex_status = ef1.selectbox("Status", ["Ativos"] + STATUS_OPCOES,
                                     key="f_ex_status", label_visibility="collapsed")
        f_ex_resp   = ef2.selectbox("Resp.", ["Todos"] + equipe_nomes,
                                     key="f_ex_resp", label_visibility="collapsed")
        if ef3.button("🔄", key="btn_extras_refresh"):
            carregar_tarefas_extras.clear(); st.rerun()

        te = list(extras_lista)
        if f_ex_status == "Ativos":
            te = [t for t in te if t["status"] not in ("Concluído","N/A")]
        elif f_ex_status != "Todos":
            te = [t for t in te if t["status"] == f_ex_status]
        if f_ex_resp != "Todos":
            te = [t for t in te if (t.get("responsavel") or {}).get("nome") == f_ex_resp]

        te = sorted(te, key=lambda t: (-(t.get("gut_score") or 1),
                                        t.get("entrega_prevista") or "9999-99-99"))

        st.caption(f"**{len(te)}** extra(s) · clique em uma linha para editar")

        if not te:
            st.info("Nenhuma tarefa extra para os filtros selecionados.")
        else:
            rows_ex = []
            for t in te:
                rows_ex.append({
                    "_id":       t["id"],
                    "GUT":       gut_emoji(t.get("gut_score") or 1),
                    "Origem":    t.get("origem") or "—",
                    "Descrição": f"{calcular_farol(t.get('entrega_prevista'), t['status'], _hoje_e)} {t.get('descricao') or '—'}",
                    "Resp.":     (t.get("responsavel") or {}).get("nome","—"),
                    "Status":    t["status"],
                    "Desvio":    calcular_desvio(t.get("entrega_prevista"), t.get("entrega_real"), t["status"], _hoje_e),
                    "Av.%":      t.get("avanco_percent") or 0,
                })
            df_ex = pd.DataFrame(rows_ex)
            sel_ex = st.dataframe(
                df_ex.drop(columns=["_id"]),
                use_container_width=True, hide_index=True,
                height=min(400, 36 + 35 * len(df_ex)),
                on_select="rerun", selection_mode="single-row",
                column_config={
                    "GUT":       st.column_config.TextColumn("GUT", width="small"),
                    "Origem":    st.column_config.TextColumn("Origem", width="medium"),
                    "Descrição": st.column_config.TextColumn("Descrição", width="large"),
                    "Resp.":     st.column_config.TextColumn("Resp.", width="medium"),
                    "Status":    st.column_config.TextColumn("Status", width="medium"),
                    "Desvio":    st.column_config.TextColumn("Desvio", width="small"),
                    "Av.%":      st.column_config.ProgressColumn("Av.%", min_value=0, max_value=100, width="small"),
                }
            )
            linhas_ex = sel_ex.selection.rows if sel_ex.selection else []
            if linhas_ex:
                te_sel = te[linhas_ex[0]]
                st.divider()
                st.markdown(f"**✏️ Extra:** {te_sel.get('descricao','')}")
                xe1, xe2 = st.columns(2)
                novo_st_ex = xe1.selectbox("Status", STATUS_OPCOES,
                    index=STATUS_OPCOES.index(te_sel["status"]) if te_sel["status"] in STATUS_OPCOES else 0,
                    key=f"ex_st_{te_sel['id']}")
                novo_av_ex = xe2.slider("% Avanço", 0, 100,
                    te_sel.get("avanco_percent") or 0, step=5, key=f"ex_av_{te_sel['id']}")
                novo_imp_ex = te_sel.get("impedimento") or ""
                if novo_st_ex == "Impedido":
                    novo_imp_ex = st.text_input("🚧 Impedimento", value=novo_imp_ex,
                                                 key=f"ex_imp_{te_sel['id']}")
                xd1, xd2 = st.columns(2)
                novo_ent_ex = xd1.date_input("Entrega prevista",
                    value=parse_date(te_sel.get("entrega_prevista")),
                    key=f"ex_ent_{te_sel['id']}", format="DD/MM/YYYY")
                novo_real_ex = xd2.date_input("Entrega real",
                    value=parse_date(te_sel.get("entrega_real")),
                    key=f"ex_real_{te_sel['id']}", format="DD/MM/YYYY")
                novo_obs_ex = st.text_area("Obs.", value=te_sel.get("observacoes") or "",
                    key=f"ex_obs_{te_sel['id']}", height=50)
                if st.button("💾 Salvar", type="primary", key=f"ex_save_{te_sel['id']}"):
                    supabase.table("obras_tarefas").update({
                        "status":           novo_st_ex,
                        "impedimento":      novo_imp_ex or None,
                        "avanco_percent":   novo_av_ex,
                        "entrega_prevista": novo_ent_ex.isoformat()  if novo_ent_ex  else None,
                        "entrega_real":     novo_real_ex.isoformat() if novo_real_ex else None,
                        "observacoes":      novo_obs_ex or None,
                    }).eq("id", te_sel["id"]).execute()
                    limpar_cache(); st.success("✅ Salvo!"); st.rerun()

    # ════════════════════════════════════════════════════════
    # ABA DASHBOARD DA OBRA
    # ════════════════════════════════════════════════════════
    with aba_dash:
        _go = go

        with st.spinner("Carregando dashboard..."):
            _df_oc   = carregar_obras_completo()
            _df_med  = carregar_medicoes_resumo()
            _df_fab  = carregar_producao_resumo()
            _df_tra  = carregar_transporte_resumo()
            _df_mont = carregar_montagem_resumo()

        # Linha da obra selecionada
        _obra_rows = _df_oc[_df_oc["id"] == obra_id]
        if _obra_rows.empty:
            st.info("Dados financeiros não disponíveis para esta obra. Importe a rota 12.")
        else:
            _orow = _obra_rows.iloc[0]

            # ── Cabeçalho ─────────────────────────────────────
            _ch1, _ch2 = st.columns([3, 2])
            with _ch1:
                _st_icon = {"Em Andamento":"🔵","Concluída":"🟢","Cancelada":"🔴","Proposta":"🟡"}.get(_orow.get("status",""),"⚪")
                st.markdown(
                    f"### {_orow.get('codigo','?')} — {_orow.get('nome','?')}\n"
                    f"{_st_icon} **{_orow.get('status','—')}** "
                    f"· {_orow.get('modalidade','—')} "
                    f"· {_orow.get('cliente','—') or '—'}")

            _fat_tot = float(_orow.get("faturamento_total") or 0)
            _med_obra = _df_med[_df_med["obra_id"] == obra_id]["valor"].sum() if not _df_med.empty else 0.0
            _saldo_o = _fat_tot - _med_obra
            _pct_fin = _med_obra / _fat_tot if _fat_tot > 0 else 0.0
            with _ch2:
                _cm1, _cm2, _cm3 = st.columns(3)
                _cm1.metric("💼 Contratado",  fmt_brl(_fat_tot))
                _cm2.metric("✅ Faturado",     fmt_brl(_med_obra))
                _cm3.metric("📋 Saldo",        fmt_brl(_saldo_o))

            st.progress(min(_pct_fin, 1.0), text=f"Avanço financeiro: {_pct_fin*100:.1f}%")

            st.divider()

            # ── Avanço físico ─────────────────────────────────
            st.markdown("#### 📐 Avanço físico")
            _dfb_o  = _df_fab[_df_fab["obra_id"]  == obra_id] if not _df_fab.empty  else pd.DataFrame()
            _dft_o  = _df_tra[_df_tra["obra_id"]  == obra_id] if not _df_tra.empty  else pd.DataFrame()
            _dfm_o  = _df_mont[_df_mont["obra_id"] == obra_id] if not _df_mont.empty else pd.DataFrame()

            _vfab  = float(_dfb_o["volume_teorico"].sum())  if not _dfb_o.empty  else 0.0
            _vtra  = float(_dft_o["volume_real"].sum())     if not _dft_o.empty  else 0.0
            _vmon  = float(_dfm_o["volume_teorico"].sum())  if not _dfm_o.empty  else 0.0
            _vref  = volume_ref(_orow)

            _pa1, _pa2, _pa3 = st.columns(3)
            for _col, _lbl, _vol in [(_pa1,"🏭 Fabricação",_vfab),
                                      (_pa2,"🚛 Expedição", _vtra),
                                      (_pa3,"🏗️ Montagem",  _vmon)]:
                with _col:
                    st.metric(_lbl, f"{_vol:,.1f} m³".replace(",","."))
                    if _vref > 0:
                        _pct_v = min(_vol / _vref, 1.0)
                        st.progress(_pct_v)
                        st.caption(f"{_vol/_vref*100:.1f}% do contrato ({_vref:,.1f} m³)".replace(",","."))
                    else:
                        st.progress(0.0)
                        st.caption("Volume de ref. não disponível")

            st.caption(
                f"📦 Pátio: {max(0,_vfab-_vtra):,.1f} m³  "
                f"| 🏚️ Canteiro: {max(0,_vtra-_vmon):,.1f} m³".replace(",","."))

            st.divider()

            # ── Evolução mensal (3 séries) ────────────────────
            st.markdown("#### 📈 Evolução mensal (m³)")
            if not _dfb_o.empty or not _dft_o.empty or not _dfm_o.empty:
                _fab_m  = _dfb_o.groupby("mes")["volume_teorico"].sum() if not _dfb_o.empty else pd.Series(dtype=float)
                _tra_m  = _dft_o.groupby("mes")["volume_real"].sum()    if not _dft_o.empty else pd.Series(dtype=float)
                _mon_m  = _dfm_o.groupby("mes")["volume_teorico"].sum() if not _dfm_o.empty else pd.Series(dtype=float)
                _todos_m = sorted(set(list(_fab_m.index)+list(_tra_m.index)+list(_mon_m.index)))
                _todos_m = [m for m in _todos_m if m and m not in ("NaT","nan")]
                if _todos_m:
                    _ev_df = pd.DataFrame({"mes": _todos_m})
                    _ev_df["Fabricação"] = _ev_df["mes"].map(_fab_m).fillna(0)
                    _ev_df["Expedição"]  = _ev_df["mes"].map(_tra_m).fillna(0)
                    _ev_df["Montagem"]   = _ev_df["mes"].map(_mon_m).fillna(0)
                    _fig_ev = _go.Figure()
                    for _nm, _cor in [("Fabricação","#1976D2"),("Expedição","#43A047"),("Montagem","#E53935")]:
                        _fig_ev.add_trace(_go.Scatter(
                            x=_ev_df["mes"], y=_ev_df[_nm],
                            mode="lines+markers", name=_nm,
                            line=dict(color=_cor, width=2),
                            hovertemplate=f"<b>{_nm}</b>: %{{y:,.1f}} m³<extra></extra>"))
                    _fig_ev.update_layout(
                        yaxis=dict(title="m³", tickformat=",.0f"),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                        margin=dict(t=50,b=20), height=280, hovermode="x unified")
                    st.plotly_chart(_fig_ev, use_container_width=True, key="dash_ev_obra")
            else:
                st.info("Sem dados de produção para esta obra.")

            st.divider()

            # ── Tipologia de produtos ─────────────────────────
            st.subheader("🔍 Tipologia de Produtos")
            _tp_fab, _tp_exp, _tp_mon = st.tabs(["Fabricação","Expedição","Montagem"])

            with _tp_fab:
                if not _dfb_o.empty:
                    _pf = (_dfb_o.groupby("produto").agg(
                        volume=("volume_teorico","sum"),
                        frouxo=("peso_aco_frouxo","sum"),
                        protendido=("peso_aco_protendido","sum"),
                    ).reset_index().sort_values("volume", ascending=False))
                    _pf["kg_m3"] = (_pf["frouxo"] + _pf["protendido"]) / _pf["volume"].replace(0, float("nan"))
                    _tf1, _tf2 = st.columns(2)
                    with _tf1:
                        _fig_pf = _go.Figure(_go.Bar(
                            x=_pf["volume"], y=_pf["produto"], orientation="h",
                            marker_color="#1976D2",
                            hovertemplate="%{y}: %{x:,.1f} m³<extra></extra>"))
                        _fig_pf.update_layout(
                            xaxis=dict(title="m³", tickformat=",.0f"),
                            yaxis=dict(autorange="reversed"),
                            margin=dict(t=10,b=10,r=10), height=300)
                        st.plotly_chart(_fig_pf, use_container_width=True, key="dash_prod_fab")
                    with _tf2:
                        st.dataframe(
                            _pf[["produto","volume","frouxo","protendido","kg_m3"]].rename(columns={
                                "produto":"Produto","volume":"Vol m³",
                                "frouxo":"Frouxo kg","protendido":"Prot. kg","kg_m3":"kg/m³"}),
                            use_container_width=True, hide_index=True, height=300)
                else:
                    st.info("Sem dados de fabricação para esta obra.")

            with _tp_exp:
                if not _dft_o.empty:
                    _pe = (_dft_o.groupby("produto")["volume_real"].sum()
                           .reset_index().sort_values("volume_real", ascending=False))
                    _fig_pe = _go.Figure(_go.Bar(
                        x=_pe["volume_real"], y=_pe["produto"], orientation="h",
                        marker_color="#43A047",
                        hovertemplate="%{y}: %{x:,.1f} m³<extra></extra>"))
                    _fig_pe.update_layout(
                        xaxis=dict(title="m³ expedido", tickformat=",.0f"),
                        yaxis=dict(autorange="reversed"),
                        margin=dict(t=10,b=10,r=10), height=280)
                    st.plotly_chart(_fig_pe, use_container_width=True, key="dash_prod_exp")
                else:
                    st.info("Sem dados de expedição para esta obra.")

            with _tp_mon:
                if not _dfm_o.empty:
                    _pm = (_dfm_o.groupby("produto")["volume_teorico"].sum()
                           .reset_index().sort_values("volume_teorico", ascending=False))
                    _fig_pm = _go.Figure(_go.Bar(
                        x=_pm["volume_teorico"], y=_pm["produto"], orientation="h",
                        marker_color="#E53935",
                        hovertemplate="%{y}: %{x:,.1f} m³<extra></extra>"))
                    _fig_pm.update_layout(
                        xaxis=dict(title="m³ montado", tickformat=",.0f"),
                        yaxis=dict(autorange="reversed"),
                        margin=dict(t=10,b=10,r=10), height=280)
                    st.plotly_chart(_fig_pm, use_container_width=True, key="dash_prod_mon")
                else:
                    st.info("Sem dados de montagem para esta obra.")

            st.divider()

            # ── % expedido e montado por produto ─────────────
            if not _dfb_o.empty:
                st.markdown("#### 📊 Avanço por produto")
                _fab_prod = _dfb_o.groupby("produto")["volume_teorico"].sum()
                _tra_prod = _dft_o.groupby("produto")["volume_real"].sum() if not _dft_o.empty else pd.Series(dtype=float)
                _mon_prod = _dfm_o.groupby("produto")["volume_teorico"].sum() if not _dfm_o.empty else pd.Series(dtype=float)
                _prods = sorted(_fab_prod.index)
                _rows_av = []
                for _p in _prods:
                    _f = float(_fab_prod.get(_p, 0))
                    _t = float(_tra_prod.get(_p, 0))
                    _m = float(_mon_prod.get(_p, 0))
                    _rows_av.append({
                        "Produto":  _p,
                        "Fab m³":   round(_f, 1),
                        "Exp m³":   round(_t, 1),
                        "Mont m³":  round(_m, 1),
                        "% Exp":    min(round(_t / _f * 100, 1) if _f else 0, 100.0),
                        "% Mont":   min(round(_m / _f * 100, 1) if _f else 0, 100.0),
                    })
                _df_av = pd.DataFrame(_rows_av).sort_values("Fab m³", ascending=False)
                st.dataframe(
                    _df_av,
                    use_container_width=True, hide_index=True,
                    height=min(400, 36 + 35 * len(_df_av)),
                    column_config={
                        "% Exp":  st.column_config.ProgressColumn("% Expedido", min_value=0, max_value=100),
                        "% Mont": st.column_config.ProgressColumn("% Montado",  min_value=0, max_value=100),
                    })

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

    # ── FUNÇÕES DE CARGA ────────────────────────────────────

    @st.cache_data(ttl=600)
    def carregar_obras_financeiro():
        resp_of = supabase.table("obras_financeiro").select("*").execute()
        if not resp_of.data:
            return pd.DataFrame()
        resp_o = supabase.table("obras")\
            .select("id, cod4, nome, status, modalidade, cliente").execute()
        df_of = pd.DataFrame(resp_of.data)
        df_o  = pd.DataFrame(resp_o.data or [])
        if not df_o.empty and "cod4" in df_of.columns:
            # Rename obras cols before merge to avoid collision with
            # obras_financeiro cols (e.g. obra_nome already exists there)
            df_o_ren = df_o.rename(columns={
                "id":     "obra_id",
                "nome":   "obra_nome",
                "status": "obra_status",
            })
            # Drop columns from df_of that will come from obras (avoid duplicates)
            for _col in ["obra_id", "obra_nome", "obra_status", "modalidade", "cliente"]:
                if _col in df_of.columns:
                    df_of = df_of.drop(columns=[_col])
            df = df_of.merge(df_o_ren, on="cod4", how="left")
        else:
            df = df_of.copy()
            for col in ("obra_id", "obra_nome", "obra_status", "modalidade", "cliente"):
                if col not in df.columns:
                    df[col] = None
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

    @st.cache_data(ttl=600)
    def carregar_medicoes():
        # Filtragem de tipo feita em Python — not_.in_ combinado com range()
        # causa APIError em certas versões do supabase-py
        df = fetch_all("medicoes", "obra_id, data_emissao, descricao, tipo, valor")
        if df.empty: return df
        excluir = {"NOTA FISCAL CANCELADA", "NOTA FISCAL REMESSA"}
        df = df[~df["tipo"].str.strip().str.upper().isin(excluir)].copy()
        df["valor"]        = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
        df["mes"]          = df["data_emissao"].dt.to_period("M").astype(str)
        return df

    @st.cache_data(ttl=600)
    def carregar_medicoes_completas(obra_id):
        df = fetch_all("medicoes", "*", obra_id=obra_id)
        if df.empty: return df
        df = df[df["tipo"].str.strip().str.upper() != "NOTA FISCAL CANCELADA"].copy()
        df["valor"]        = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
        df = df.sort_values("data_emissao", ascending=False).reset_index(drop=True)
        return df

    def _carregar_volumes(tabela, col_vol, col_data):
        """Carrega (obra_id, peca, col_vol) com deduplicação por (obra_id, peca).
        Deduplicar evita contagem dobrada quando a importação foi executada mais de uma vez."""
        df = fetch_all(tabela, f"obra_id, peca, {col_vol}")
        if df.empty: return {}
        df[col_vol] = pd.to_numeric(df[col_vol], errors="coerce").fillna(0)
        return df.groupby("obra_id")[col_vol].sum().to_dict()

    @st.cache_data(ttl=900)
    def volume_fabricado_por_obra():
        df = pd.DataFrame(
            supabase.table("mv_fabricacao_mensal")
            .select("obra_id, vol_teorico").execute().data or []
        )
        if df.empty:
            return {}
        df["vol_teorico"] = pd.to_numeric(df["vol_teorico"], errors="coerce").fillna(0)
        return df.groupby("obra_id")["vol_teorico"].sum().to_dict()

    @st.cache_data(ttl=900)
    def volume_expedido_por_obra():
        df = pd.DataFrame(
            supabase.table("mv_transporte_mensal")
            .select("obra_id, vol_real").execute().data or []
        )
        if df.empty:
            return {}
        df["vol_real"] = pd.to_numeric(df["vol_real"], errors="coerce").fillna(0)
        return df.groupby("obra_id")["vol_real"].sum().to_dict()

    @st.cache_data(ttl=900)
    def volume_montado_por_obra():
        df = pd.DataFrame(
            supabase.table("mv_montagem_mensal")
            .select("obra_id, vol_montado").execute().data or []
        )
        if df.empty:
            return {}
        df["vol_montado"] = pd.to_numeric(df["vol_montado"], errors="coerce").fillna(0)
        return df.groupby("obra_id")["vol_montado"].sum().to_dict()

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
        limpar_todos_caches()
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
        _st_opts_fin = ["Todos"] + sorted(df_of["obra_status"].dropna().unique().tolist())
        _st_def_fin  = _st_opts_fin.index("Em Andamento") if "Em Andamento" in _st_opts_fin else 0
        sel_ob_st = ob1.selectbox("Status", _st_opts_fin, index=_st_def_fin,
                                   key="fin_obra_status", label_visibility="collapsed")

        df_ob_disp = df_of.copy()
        if sel_ob_st != "Todos":
            df_ob_disp = df_ob_disp[df_ob_disp["obra_status"] == sel_ob_st]
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

        # Avanço físico (fabricação, expedição, montagem) vs financeiro
        st.markdown("#### 📐 Avanço físico vs financeiro")
        vref, vref_origem = vol_ref_row(row_obra)

        vol_fab_o = float(volume_fabricado_por_obra().get(obra_id_f, 0))
        vol_exp_o = float(volume_expedido_por_obra().get(obra_id_f, 0))
        vol_mon_o = float(volume_montado_por_obra().get(obra_id_f, 0))

        def _pct(vol, ref):
            return min(vol / ref, 1.0) if ref and ref > 0 else 0.0

        pct_fab = _pct(vol_fab_o, vref)
        pct_exp = _pct(vol_exp_o, vref)
        pct_mon = _pct(vol_mon_o, vref)
        pct_fis = pct_fab  # referência para gap vs financeiro

        av1, av2, av3, av4 = st.columns(4)

        with av1:
            st.markdown("**🏭 Fabricação**")
            if vref and vref > 0:
                st.progress(pct_fab)
                st.caption(
                    f"{vol_fab_o:,.1f} / {vref:,.1f} m³ ({vref_origem})"
                    f" — **{pct_fab*100:.1f}%**".replace(",", "."))
            else:
                st.progress(0.0)
                st.caption("Ref. não disponível")

        with av2:
            st.markdown("**🚛 Expedição**")
            if vref and vref > 0:
                st.progress(pct_exp)
                st.caption(
                    f"{vol_exp_o:,.1f} / {vref:,.1f} m³"
                    f" — **{pct_exp*100:.1f}%**".replace(",", "."))
            elif vol_fab_o > 0:
                # Sem vref: mostra % sobre fabricado
                pct_exp_fab = _pct(vol_exp_o, vol_fab_o)
                st.progress(pct_exp_fab)
                st.caption(
                    f"{vol_exp_o:,.1f} / {vol_fab_o:,.1f} m³ fab."
                    f" — **{pct_exp_fab*100:.1f}%**".replace(",", "."))
            else:
                st.progress(0.0)
                st.caption("Sem dados de fabricação")

        with av3:
            st.markdown("**🏗️ Montagem**")
            if vref and vref > 0:
                st.progress(pct_mon)
                st.caption(
                    f"{vol_mon_o:,.1f} / {vref:,.1f} m³"
                    f" — **{pct_mon*100:.1f}%**".replace(",", "."))
            elif vol_fab_o > 0:
                pct_mon_fab = _pct(vol_mon_o, vol_fab_o)
                st.progress(pct_mon_fab)
                st.caption(
                    f"{vol_mon_o:,.1f} / {vol_fab_o:,.1f} m³ fab."
                    f" — **{pct_mon_fab*100:.1f}%**".replace(",", "."))
            else:
                st.progress(0.0)
                st.caption("Sem dados de fabricação")

        with av4:
            st.markdown("**💰 Financeiro**")
            st.progress(min(pct_fin_o, 1.0))
            st.caption(
                f"{fmt_brl_fin(faturado_o)} / {fmt_brl_fin(fat_total_o)}"
                f" — **{pct_fin_o*100:.1f}%**")

        # Gap fabricação vs financeiro
        if vref and vref > 0 and fat_total_o > 0:
            diff = pct_fis * 100 - pct_fin_o * 100
            valor_a_fat = (pct_fis - pct_fin_o) * fat_total_o
            if abs(diff) < 5:
                st.success(f"✅ Fabricação e financeiro alinhados ({diff:+.1f}%)")
            elif diff > 0:
                st.info(
                    f"📐 Fabricação à frente do financeiro em {diff:.1f}%"
                    f" — {fmt_brl_fin(valor_a_fat)} a faturar")
            else:
                st.warning(f"💰 Financeiro à frente da fabricação em {abs(diff):.1f}%")

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
            cols_nf = [c for c in ["data_emissao", "numero_nf", "descricao", "tipo", "valor"]
                       if c in df_nfs.columns]
            df_nf_show = df_nfs[cols_nf].copy()
            df_nf_show["valor_fmt"] = df_nf_show["valor"].apply(fmt_brl_fin)
            st.dataframe(
                df_nf_show.drop(columns=["valor"]).rename(columns={
                    "data_emissao": "Data",
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

    # ── FUNÇÕES DE CARGA ────────────────────────────────────
    @st.cache_data(ttl=900)
    def carregar_fabricacao(inicio, fim):
        def _query(cols):
            rows, page, size = [], 0, 5000
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
            rows = _query("obra_id, peca, produto, etapa, volume_teorico, volume_total,"
                          " peso_aco, peso_aco_frouxo, peso_aco_protendido, data_fabricacao")
        except Exception:
            rows = _query("obra_id, peca, produto, etapa, volume_teorico, volume_total, peso_aco, data_fabricacao")

        df = pd.DataFrame(rows)
        if df.empty: return df
        for col in ["volume_teorico", "volume_total", "peso_aco", "peso_aco_frouxo", "peso_aco_protendido"]:
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

    @st.cache_data(ttl=900)
    def carregar_transporte_prod(inicio, fim):
        rows, page, size = [], 0, 5000
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

    @st.cache_data(ttl=900)
    def carregar_montagem_prod(inicio, fim):
        rows, page, size = [], 0, 5000
        q = (supabase.table("producao_montagem")
             .select("obra_id, peca, produto, etapa, volume_total, data_montagem"))
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

    @st.cache_data(ttl=900)
    def carregar_patio(inicio, fim):
        """Carrega peca+obra+datas para calcular prazo de pátio (fab → expedição)."""
        rows_f, page, size = [], 0, 5000
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

    @st.cache_data(ttl=900)
    def carregar_custos_prod():
        """Carrega custos com conta_gerencial para análise de eficiência R$/m³."""
        df = fetch_all("custos",
                       "data, centro_custos, conta_gerencial, conta_macro, valor_global")
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

    hoje = date.today()

    f1, f2, f3, f4, f5 = st.columns([2, 2, 1, 4, 1])
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

    _st_opts_prod = ["Todos"] + sorted(set(o.get("status") or "—" for o in obras_lista))
    _st_def_prod  = _st_opts_prod.index("Em Andamento") if "Em Andamento" in _st_opts_prod else 0
    _st_sel_prod  = f3.selectbox("Status", _st_opts_prod, index=_st_def_prod,
                                  key="prod_status_sel", label_visibility="collapsed")
    obras_prod_fil = obras_lista if _st_sel_prod == "Todos" else [o for o in obras_lista if o.get("status") == _st_sel_prod]
    obras_map = {o["id"]: f"{o['cod4']} — {o['nome']}" for o in obras_prod_fil}

    sel_obras = f4.multiselect("Obras", list(obras_map.values()), key="prod_obras",
                                placeholder="Todas as obras")
    if f5.button("🔄 Atualizar", key="btn_prod_refresh"):
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
        vol_fab = df_fab_f["volume_teorico"].sum() if not df_fab_f.empty else 0
        vol_exp = df_tra_f["volume_real"].sum()    if not df_tra_f.empty else 0
        vol_mon = df_mon_f["volume_total"].sum()   if not df_mon_f.empty else 0
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

        df_ev_fab = agg_mes(df_fab_f, "volume_teorico", "Fabricação")
        df_ev_tra = agg_mes(df_tra_f, "volume_real",   "Expedição")
        df_ev_mon = agg_mes(df_mon_f, "volume_total",  "Montagem")

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

        fab_obra = (df_fab_f[df_fab_f["obra_id"].notna()].groupby("obra_id")["volume_teorico"].sum()
                    if not df_fab_f.empty else pd.Series(dtype=float))
        tra_obra = (df_tra_f[df_tra_f["obra_id"].notna()].groupby("obra_id")["volume_real"].sum()
                    if not df_tra_f.empty else pd.Series(dtype=float))
        mon_obra = (df_mon_f[df_mon_f["obra_id"].notna()].groupby("obra_id")["volume_total"].sum()
                    if not df_mon_f.empty else pd.Series(dtype=float))

        # Mapa cod4 → nome para cobrir registros sem obra_id
        _cod4_nome_map = {}
        for _o in obras_lista:
            if _o.get("cod4") and _o.get("nome"):
                _cod4_nome_map[str(_o["cod4"])] = f"{_o['cod4']} — {_o['nome']}"

        # Agrega por cod4_obra para cobrir obra_id NULL
        def _vol_por_cod4(df, col):
            if df.empty: return pd.Series(dtype=float)
            return (df[df["obra_id"].isna()]
                    .dropna(subset=["cod4_obra"])
                    .groupby("cod4_obra")[col].sum())

        fab_cod4 = _vol_por_cod4(df_fab_f, "volume_teorico")
        tra_cod4 = _vol_por_cod4(df_tra_f, "volume_real")
        mon_cod4 = _vol_por_cod4(df_mon_f, "volume_total")

        todas_ids = sorted(set(
            [int(x) for x in list(fab_obra.index) + list(tra_obra.index) + list(mon_obra.index)
             if x is not None]))
        todas_cod4 = sorted(set(
            list(fab_cod4.index) + list(tra_cod4.index) + list(mon_cod4.index)))

        rows_obra = []
        for oid in todas_ids:
            fab = float(fab_obra.get(oid, 0))
            exp = float(tra_obra.get(oid, 0))
            mon = float(mon_obra.get(oid, 0))
            gap = max(0.0, fab - exp)
            if   gap > 500: farol = "🔴"
            elif gap > 200: farol = "🟡"
            else:           farol = "🟢"
            # Tenta resolver nome via obras_map, depois via cod4
            _nome_oid = obras_map.get(oid)
            if not _nome_oid:
                _cod4_do_oid = next(
                    (str(_o["cod4"]) for _o in obras_lista if _o["id"] == oid), "")
                _nome_oid = _cod4_nome_map.get(_cod4_do_oid, str(oid))
            rows_obra.append({
                "Obra":       _nome_oid,
                "Fab. m³":    round(fab, 1),
                "% Expedido": min(round(exp / fab * 100, 1) if fab else 0, 100.0),
                "% Montado":  min(round(mon / fab * 100, 1) if fab else 0, 100.0),
                "Pátio m³":   round(gap, 1),
                "Pátio":      f"{farol} {gap:,.0f} m³".replace(",", "."),
            })

        for cod4 in todas_cod4:
            fab = float(fab_cod4.get(cod4, 0))
            exp = float(tra_cod4.get(cod4, 0))
            mon = float(mon_cod4.get(cod4, 0))
            gap = max(0.0, fab - exp)
            if   gap > 500: farol = "🔴"
            elif gap > 200: farol = "🟡"
            else:           farol = "🟢"
            rows_obra.append({
                "Obra":       _cod4_nome_map.get(str(cod4), str(cod4)),
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
                top_prod = (df_fab_f.groupby("produto")["volume_teorico"]
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
                etapa_vol = (df_fab_f.groupby("etapa")["volume_teorico"]
                             .sum().sort_values(ascending=False).reset_index())
                etapa_vol = etapa_vol[etapa_vol["volume_teorico"] > 0]
                if not etapa_vol.empty:
                    fig_etapa = go.Figure(go.Pie(
                        labels=etapa_vol["etapa"],
                        values=etapa_vol["volume_teorico"],
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
            vol_mes_fab = (df_fab_f.groupby("mes")["volume_teorico"].sum()
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
        vol_total_fab = df_fab_f["volume_teorico"].sum() if not df_fab_f.empty else 0

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
            @st.cache_data(ttl=900)
            def carregar_fabricacao_historico():
                try:
                    df = fetch_all("producao_fabricacao",
                                   "obra_id, produto, volume_teorico, peso_aco,"
                                   " peso_aco_frouxo, peso_aco_protendido")
                except Exception:
                    df = fetch_all("producao_fabricacao",
                                   "obra_id, produto, volume_teorico, peso_aco")
                if df.empty: return df
                for col in ["volume_teorico", "peso_aco", "peso_aco_frouxo", "peso_aco_protendido"]:
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
                ref_prod = (df_hist[df_hist["volume_teorico"] > 0]
                            .groupby("produto")
                            .agg(aco_h=("peso_aco", "sum"), vol_h=("volume_teorico", "sum"))
                            .reset_index())
                ref_prod["ref_kgm3"] = ref_prod["aco_h"] / ref_prod["vol_h"]
            else:
                ref_prod = pd.DataFrame(columns=["produto", "ref_kgm3"])

            bench = (df_fab_f[df_fab_f["volume_teorico"] > 0]
                     .groupby("produto")
                     .agg(frouxo=("peso_aco_frouxo", "sum"),
                          cv=("cord_viga",           "sum"),
                          cl=("cord_laje",           "sum"),
                          aco=("peso_aco",           "sum"),
                          vol=("volume_teorico",     "sum"))
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
                            .agg(aco=("peso_aco", "sum"), vol=("volume_teorico", "sum"))
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
        # ==========================================================
# PÁGINA: JORNADA DA OBRA (VISÃO MACRO E GANTT)
# ==========================================================
elif pagina_selecionada == "🛤️ Jornada da Obra":

    st.header("🛤️ Jornada do Contrato e Marcos")

    # ── FUNÇÕES DE CARGA ESPECÍFICAS DA JORNADA ──────────────
    @st.cache_data(ttl=60)
    def carregar_jornada(obra_id):
        resp = supabase.table("obra_jornada")\
            .select("*, template:jornada_template(*), responsavel:equipe(nome)")\
            .eq("obra_id", obra_id).execute()
        return sorted(resp.data, key=lambda x: x["template"]["ordem"]) if resp.data else []

    @st.cache_data(ttl=60)
    def carregar_marcos(obra_id):
        resp = supabase.table("obra_marcos").select("*").eq("obra_id", obra_id).execute()
        return resp.data[0] if resp.data else {}

    def instanciar_jornada(obra_id, modalidade):
        """Cria as tarefas da jornada para a obra, filtrando pela modalidade."""
        templates = supabase.table("jornada_template").select("*").execute().data
        if not templates: return False
        
        pacote = []
        for t in templates:
            # Filtro inteligente de aplicabilidade
            aplica = True
            if modalidade == "FOB" and not t["aplica_fob"]: aplica = False
            if modalidade == "CIF" and not t["aplica_cif"]: aplica = False
            if modalidade == "Montagem" and not t["aplica_montagem"]: aplica = False
            
            pacote.append({
                "obra_id": obra_id,
                "template_id": t["id"],
                "aplicavel": aplica,
                "status": "nao_iniciado"
            })
        try:
            supabase.table("obra_jornada").insert(pacote).execute()
            
            # Instancia a linha de marcos vazia apenas se não existir
            marcos_existentes = supabase.table("obra_marcos").select("id").eq("obra_id", obra_id).execute().data
            if not marcos_existentes:
                supabase.table("obra_marcos").insert({"obra_id": obra_id}).execute()
            return True
        except Exception as e:
            st.error(f"Erro ao instanciar: {e}")
            return False

    # ── MAPEAMENTO DE STATUS ─────────────────────────────────
    MAP_ST = {
        "nao_iniciado": ("⬜", "Não iniciado"),
        "em_andamento": ("🔵", "Em andamento"),
        "concluido":    ("✅", "Concluído"),
        "impedido":     ("🔴", "Impedido")
    }

    def derivar_status_etapa(itens):
        status_list = [i["status"] for i in itens if i["aplicavel"]]
        if not status_list: return "⬜", "Não aplicável"
        
        if "impedido" in status_list: return "🔴", "Impedida"
        if all(s == "concluido" for s in status_list): return "✅", "Concluída"
        if all(s == "nao_iniciado" for s in status_list): return "⬜", "Não iniciada"
        return "🔵", "Em andamento"

    # ── SELEÇÃO DE OBRA ──────────────────────────────────────
    obras = carregar_obras_ativas()
    if not obras:
        st.warning("Nenhuma obra cadastrada."); st.stop()

    c1, c2, c3 = st.columns([1, 3, 1])
    _st_opts_jor = ["Todos"] + sorted(set(o.get("status") or "—" for o in obras))
    _st_def_jor  = _st_opts_jor.index("Em Andamento") if "Em Andamento" in _st_opts_jor else 0
    _st_sel_jor  = c1.selectbox("Status", _st_opts_jor, index=_st_def_jor,
                                 key="jor_status_sel", label_visibility="collapsed")
    obras_jor = obras if _st_sel_jor == "Todos" else [o for o in obras if o.get("status") == _st_sel_jor]
    if not obras_jor:
        st.warning("Nenhuma obra para o status selecionado."); st.stop()

    obras_map = {f"{o['cod4']} — {o['nome']}": o for o in obras_jor}
    obra_sel_str = c2.selectbox("Selecione a Obra", list(obras_map.keys()), label_visibility="collapsed")
    obra_sel = obras_map[obra_sel_str]
    obra_id = obra_sel["id"]
    modalidade = obra_sel.get("modalidade") or "Montagem"

    if c3.button("🔄 Atualizar Dados", use_container_width=True):
        carregar_jornada.clear()
        carregar_marcos.clear()
        st.rerun()

    jornada = carregar_jornada(obra_id)
    
    # ── INSTANCIAÇÃO SE VAZIO ────────────────────────────────
    if not jornada:
        st.info(f"A jornada do contrato para a obra **{obra_sel['nome']}** ainda não foi iniciada.")
        st.write(f"Modalidade detectada: **{modalidade}**")
        if st.button("🚀 Iniciar Jornada do Contrato", type="primary"):
            with st.spinner("Gerando entregáveis e cronograma..."):
                if instanciar_jornada(obra_id, modalidade):
                    carregar_jornada.clear()
                    carregar_marcos.clear()
                    st.rerun()
                else:
                    st.error("Erro ao gerar jornada. Verifique se a tabela 'jornada_template' está preenchida no banco.")
        st.stop()

    # ── PROCESSAMENTO DA JORNADA ─────────────────────────────
    etapas = {}
    total_aplicaveis = 0
    total_concluidas = 0

    for item in jornada:
        if not item["aplicavel"]: continue
        total_aplicaveis += 1
        if item["status"] == "concluido": total_concluidas += 1
        
        nome_etapa = item["template"]["etapa_nome"]
        etapas.setdefault(nome_etapa, []).append(item)

    pct_jornada = (total_concluidas / total_aplicaveis) * 100 if total_aplicaveis else 0

    # ── BARRA DE PROGRESSO VISUAL (O MAPA DO FLUXO) ──────────
    html_etapas = []
    for nome_etapa, itens in etapas.items():
        icone, texto_status = derivar_status_etapa(itens)
        cor_borda = "#43A047" if icone == "✅" else "#E53935" if icone == "🔴" else "#1976D2" if icone == "🔵" else "#546E7A"
        opacidade = "1" if icone != "⬜" else "0.5"
        
        html_etapas.append(f"""
        <div style="display:inline-block; text-align:center; margin-right:20px; opacity:{opacidade};">
            <div style="border-bottom: 3px solid {cor_borda}; padding-bottom:5px; margin-bottom:5px;">
                <span style="font-size:1.2rem;">{icone}</span> <b>{nome_etapa}</b>
            </div>
            <div style="font-size:0.75rem; color:#888;">{texto_status}</div>
        </div>
        """)

    st.markdown(f"#### 🛤️ Mapa do Contrato — {pct_jornada:.1f}% concluído")
    st.progress(pct_jornada / 100)
    st.markdown("<div style='overflow-x:auto; white-space:nowrap; padding:10px 0;'>" + " ➔ ".join(html_etapas) + "</div>", unsafe_allow_html=True)
    
    st.divider()

    # ── ABAS: ENTREGÁVEIS vs CRONOGRAMA ──────────────────────
    aba_jornada, aba_marcos = st.tabs(["📋 Gestão dos Entregáveis", "📅 Cronograma Gantt (Pactuado vs Real)"])

    # ════════════════════════════════════════════════════════
    # ABA 1: GESTÃO DA JORNADA (ENTREGÁVEIS)
    # ════════════════════════════════════════════════════════
    with aba_jornada:
        st.caption("Expanda a etapa para atualizar o status de cada entregável.")
        equipe = carregar_equipe_ativa()
        opcoes_equipe = ["—"] + [e["nome"] for e in equipe]
        mapa_eq_ids = {e["nome"]: e["id"] for e in equipe}

        for nome_etapa, itens in etapas.items():
            icone_etapa, status_etapa = derivar_status_etapa(itens)
            concluidos_etapa = sum(1 for i in itens if i["status"] == "concluido")
            total_etapa = len(itens)
            
            with st.expander(f"{icone_etapa} {nome_etapa} ({concluidos_etapa}/{total_etapa})"):
                for item in itens:
                    t = item["template"]
                    icone_item = MAP_ST[item["status"]][0]
                    
                    st.markdown(f"**{icone_item} {t['item_cod']} - {t['descricao']}**")
                    
                    c1, c2, c3, c4 = st.columns([2, 2, 3, 1])
                    
                    novo_st = c1.selectbox("Status", list(MAP_ST.keys()), 
                                           format_func=lambda x: f"{MAP_ST[x][0]} {MAP_ST[x][1]}",
                                           index=list(MAP_ST.keys()).index(item["status"]), 
                                           key=f"st_{item['id']}")
                    
                    resp_atual = item["responsavel"]["nome"] if item.get("responsavel") else "—"
                    novo_resp = c2.selectbox("Responsável", opcoes_equipe, 
                                             index=opcoes_equipe.index(resp_atual) if resp_atual in opcoes_equipe else 0,
                                             key=f"resp_{item['id']}")
                    
                    novo_imp = c3.text_input("Impedimento / Obs", value=item["impedimento"] or item["observacao"] or "", key=f"obs_{item['id']}")
                    
                    if c4.button("💾 Salvar", key=f"btn_{item['id']}", use_container_width=True):
                        dados_update = {
                            "status": novo_st,
                            "responsavel_id": mapa_eq_ids.get(novo_resp) if novo_resp != "—" else None,
                            "updated_at": "now()"
                        }
                        if novo_st == "impedido":
                            dados_update["impedimento"] = novo_imp
                            dados_update["observacao"] = None
                        else:
                            dados_update["observacao"] = novo_imp
                            dados_update["impedimento"] = None
                            
                        if novo_st == "concluido" and item["status"] != "concluido":
                            dados_update["data_conclusao"] = date.today().isoformat()
                            
                        supabase.table("obra_jornada").update(dados_update).eq("id", item["id"]).execute()
                        carregar_jornada.clear()
                        st.success("Salvo!"); st.rerun()
                    st.write("---")

    # ════════════════════════════════════════════════════════
    # ABA 2: MARCOS E GANTT
    # ════════════════════════════════════════════════════════
    with aba_marcos:
        marcos = carregar_marcos(obra_id)
        
        def safe_date(date_str):
            if not date_str: return None
            try: return date.fromisoformat(str(date_str)[:10])
            except: return None

        st.markdown("#### 📅 Cadastro de Datas")
        with st.form("form_marcos"):
            mc1, mc2, mc3 = st.columns(3)
            
            mc1.markdown("**🏭 Fabricação**")
            f_ini_p = mc1.date_input("Início Pactuado (Fab)", value=safe_date(marcos.get("inicio_fab_pact")), format="DD/MM/YYYY")
            f_fim_p = mc1.date_input("Fim Pactuado (Fab)", value=safe_date(marcos.get("fim_fab_pact")), format="DD/MM/YYYY")
            f_ini_r = mc1.date_input("Início Real (Fab)", value=safe_date(marcos.get("inicio_fab_real")), format="DD/MM/YYYY")
            f_fim_r = mc1.date_input("Fim Real (Fab)", value=safe_date(marcos.get("fim_fab_real")), format="DD/MM/YYYY")

            mc2.markdown("**🚛 Expedição**")
            e_ini_p = mc2.date_input("Início Pactuado (Exp)", value=safe_date(marcos.get("inicio_exp_pact")), format="DD/MM/YYYY")
            e_fim_p = mc2.date_input("Fim Pactuado (Exp)", value=safe_date(marcos.get("fim_exp_pact")), format="DD/MM/YYYY")
            e_ini_r = mc2.date_input("Início Real (Exp)", value=safe_date(marcos.get("inicio_exp_real")), format="DD/MM/YYYY")
            e_fim_r = mc2.date_input("Fim Real (Exp)", value=safe_date(marcos.get("fim_exp_real")), format="DD/MM/YYYY")

            mc3.markdown("**🏗️ Montagem / Entrega**")
            m_ini_p = mc3.date_input("Início Pactuado (Mont)", value=safe_date(marcos.get("inicio_mont_pact")), format="DD/MM/YYYY")
            m_fim_p = mc3.date_input("Entrega Final Pactuada", value=safe_date(marcos.get("entrega_pact")), format="DD/MM/YYYY")
            m_ini_r = mc3.date_input("Início Real (Mont)", value=safe_date(marcos.get("inicio_mont_real")), format="DD/MM/YYYY")
            m_fim_r = mc3.date_input("Entrega Final Real", value=safe_date(marcos.get("entrega_real")), format="DD/MM/YYYY")

            if st.form_submit_button("💾 Salvar Marcos", type="primary"):
                pacote_marcos = {
                    "inicio_fab_pact": f_ini_p.isoformat() if f_ini_p else None,
                    "fim_fab_pact": f_fim_p.isoformat() if f_fim_p else None,
                    "inicio_fab_real": f_ini_r.isoformat() if f_ini_r else None,
                    "fim_fab_real": f_fim_r.isoformat() if f_fim_r else None,
                    "inicio_exp_pact": e_ini_p.isoformat() if e_ini_p else None,
                    "fim_exp_pact": e_fim_p.isoformat() if e_fim_p else None,
                    "inicio_exp_real": e_ini_r.isoformat() if e_ini_r else None,
                    "fim_exp_real": e_fim_r.isoformat() if e_fim_r else None,
                    "inicio_mont_pact": m_ini_p.isoformat() if m_ini_p else None,
                    "entrega_pact": m_fim_p.isoformat() if m_fim_p else None,
                    "inicio_mont_real": m_ini_r.isoformat() if m_ini_r else None,
                    "entrega_real": m_fim_r.isoformat() if m_fim_r else None,
                    "updated_at": "now()"
                }
                # Se não existir a linha, insere, senão atualiza
                if not marcos:
                    pacote_marcos["obra_id"] = obra_id
                    supabase.table("obra_marcos").insert(pacote_marcos).execute()
                else:
                    supabase.table("obra_marcos").update(pacote_marcos).eq("obra_id", obra_id).execute()
                
                carregar_marcos.clear()
                st.success("Marcos atualizados!"); st.rerun()

        # ── GRÁFICO DE GANTT ──────────────────────────────────────
        st.divider()
        st.markdown("#### 📊 Cronograma Comparativo (Meta vs Real)")
        
        gantt_data = []
        hoje = date.today()

        def add_gantt(etapa, inicio_p, fim_p, inicio_r, fim_r):
            # Faixa Pactuada
            if inicio_p and fim_p:
                gantt_data.append(dict(Etapa=etapa, Tipo="Meta Pactuada", Inicio=inicio_p, Fim=fim_p, Cor="#B0BEC5"))
            
            # Faixa Real
            if inicio_r:
                f_real_calc = fim_r if fim_r else hoje
                cor_real = "#43A047" # Verde (No prazo)
                
                if fim_p:
                    if fim_r and fim_r > fim_p: cor_real = "#E53935" # Vermelho
                    elif not fim_r and hoje > fim_p: cor_real = "#E53935" # Vermelho
                
                texto_tipo = "Execução Real" if fim_r else "Execução em Andamento"
                gantt_data.append(dict(Etapa=etapa, Tipo=texto_tipo, Inicio=inicio_r, Fim=f_real_calc, Cor=cor_real))

        add_gantt("1. Fabricação", safe_date(marcos.get("inicio_fab_pact")), safe_date(marcos.get("fim_fab_pact")), 
                                   safe_date(marcos.get("inicio_fab_real")), safe_date(marcos.get("fim_fab_real")))
        
        add_gantt("2. Expedição",  safe_date(marcos.get("inicio_exp_pact")), safe_date(marcos.get("fim_exp_pact")), 
                                   safe_date(marcos.get("inicio_exp_real")), safe_date(marcos.get("fim_exp_real")))
        
        add_gantt("3. Montagem",   safe_date(marcos.get("inicio_mont_pact")), safe_date(marcos.get("entrega_pact")), 
                                   safe_date(marcos.get("inicio_mont_real")), safe_date(marcos.get("entrega_real")))

        if not gantt_data:
            st.info("Preencha as datas de Início e Fim acima para visualizar o cronograma.")
        else:
            df_gantt = pd.DataFrame(gantt_data)
            
            fig_gantt = px.timeline(df_gantt, x_start="Inicio", x_end="Fim", y="Etapa", color="Tipo",
                                    color_discrete_map={"Meta Pactuada": "#B0BEC5", "Execução Real": "#43A047", "Execução em Andamento": "#E53935"},
                                    barmode="group", height=300)
            
            fig_gantt.update_yaxes(autorange="reversed")
            fig_gantt.update_layout(
                xaxis=dict(title="", tickformat="%d/%b/%y"),
                yaxis=dict(title=""),
                legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0, title=""),
                margin=dict(t=10, b=10, l=10, r=10)
            )
            
            for i, data in enumerate(fig_gantt.data):
                tipo_atual = data.name
                cores_linha = df_gantt[df_gantt["Tipo"] == tipo_atual]["Cor"].tolist()
                fig_gantt.data[i].marker.color = cores_linha
            
            st.plotly_chart(fig_gantt, use_container_width=True)
            st.caption("Cinza: Planejado | Verde: Executado dentro do prazo | Vermelho: Atrasado")

# ==========================================================
# PÁGINA: ANÁLISE DA OBRA (DASHBOARD INTEGRADO)
# ==========================================================
elif pagina_selecionada == "🔍 Análise da Obra":

    st.header("🔍 Análise da Obra")

    # ── CACHE FUNCTIONS ──────────────────────────────────────
    @st.cache_data(ttl=300)
    def _jornada_analise(obra_id):
        resp = supabase.table("obra_jornada")\
            .select("*, template:jornada_template(*)")\
            .eq("obra_id", obra_id).execute()
        return sorted(resp.data, key=lambda x: x["template"]["ordem"]) if resp.data else []

    @st.cache_data(ttl=300)
    def _marcos_analise(obra_id):
        resp = supabase.table("obra_marcos").select("*").eq("obra_id", obra_id).execute()
        return resp.data[0] if resp.data else {}

    @st.cache_data(ttl=600)
    def _fab_obra(obra_id):
        df = fetch_all("producao_fabricacao",
                       "peca, produto, volume_teorico, data_fabricacao",
                       obra_id=obra_id)
        if not df.empty:
            df["volume_teorico"] = pd.to_numeric(df["volume_teorico"], errors="coerce").fillna(0)
            df["data_fabricacao"] = pd.to_datetime(df["data_fabricacao"], errors="coerce")
        return df

    @st.cache_data(ttl=600)
    def _exp_obra(obra_id):
        df = fetch_all("producao_transporte",
                       "produto, volume_real, data_expedicao",
                       obra_id=obra_id)
        if not df.empty:
            df["volume_real"] = pd.to_numeric(df["volume_real"], errors="coerce").fillna(0)
            df["data_expedicao"] = pd.to_datetime(df["data_expedicao"], errors="coerce")
        return df

    @st.cache_data(ttl=600)
    def _mont_obra(obra_id):
        df = fetch_all("producao_montagem",
                       "produto, volume_teorico, data_montagem",
                       obra_id=obra_id)
        if not df.empty:
            df["volume_teorico"] = pd.to_numeric(df["volume_teorico"], errors="coerce").fillna(0)
            df["data_montagem"] = pd.to_datetime(df["data_montagem"], errors="coerce")
        return df

    @st.cache_data(ttl=600)
    def _medicoes_obra(obra_id):
        try:
            df = fetch_all("medicoes",
                           "numero_nf, tipo, valor, data_emissao",
                           obra_id=obra_id)
            if df.empty: return df
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
            df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
            return df
        except Exception:
            return pd.DataFrame()

    @st.cache_data(ttl=600)
    def _fin_obra(obra_id):
        try:
            r = supabase.table("obras").select("cod4").eq("id", obra_id).limit(1).execute()
            if not r.data: return {}
            cod4 = r.data[0].get("cod4")
            if not cod4: return {}
            resp = supabase.table("obras_financeiro")\
                .select("faturamento_total, volume, volume_projeto")\
                .eq("cod4", cod4).limit(1).execute()
            return resp.data[0] if resp.data else {}
        except Exception:
            return {}

    # ── HELPERS ──────────────────────────────────────────────
    _MAP_ST_AN = {
        "nao_iniciado": ("⬜", "Não iniciado"),
        "em_andamento": ("🔵", "Em andamento"),
        "concluido":    ("✅", "Concluído"),
        "impedido":     ("🔴", "Impedido"),
    }

    def _derivar_etapa(itens):
        sl = [i["status"] for i in itens if i.get("aplicavel", True)]
        if not sl: return "⬜", "N/A"
        if "impedido" in sl: return "🔴", "Impedida"
        if all(s == "concluido" for s in sl): return "✅", "Concluída"
        if all(s == "nao_iniciado" for s in sl): return "⬜", "Não iniciada"
        return "🔵", "Em andamento"

    def _safe_date(d):
        if not d: return None
        try: return date.fromisoformat(str(d)[:10])
        except: return None

    def _barra_html(pct, cor="#1976D2"):
        pct = min(max(float(pct or 0), 0), 100)
        return (f"<div style='background:#e0e0e0;border-radius:4px;height:6px;width:100%;'>"
                f"<div style='background:{cor};width:{pct:.1f}%;height:6px;border-radius:4px;'></div></div>")

    # ── NÍVEL 1: SELETOR ─────────────────────────────────────
    obras_an = carregar_obras_ativas()
    if not obras_an:
        st.warning("Nenhuma obra cadastrada."); st.stop()

    c_st_an, c_sel, c_btn = st.columns([1, 4, 1])
    _st_opts_an = ["Todos"] + sorted(set(o.get("status") or "—" for o in obras_an))
    _st_def_an  = _st_opts_an.index("Em Andamento") if "Em Andamento" in _st_opts_an else 0
    _st_sel_an  = c_st_an.selectbox("Status", _st_opts_an, index=_st_def_an,
                                     key="an_status_sel", label_visibility="collapsed")
    obras_an_fil = obras_an if _st_sel_an == "Todos" else [o for o in obras_an if o.get("status") == _st_sel_an]
    if not obras_an_fil:
        st.warning("Nenhuma obra para o status selecionado."); st.stop()

    obras_map_an = {f"{o['cod4']} — {o['nome']}": o for o in obras_an_fil}
    obra_lbl_an  = c_sel.selectbox("Selecione a Obra", list(obras_map_an.keys()),
                                   label_visibility="collapsed", key="analise_obra_sel")
    obra_an      = obras_map_an[obra_lbl_an]
    oid_an       = int(obra_an["id"])

    if c_btn.button("🔄 Atualizar", use_container_width=True, key="analise_refresh"):
        _jornada_analise.clear(); _marcos_analise.clear()
        _fab_obra.clear(); _exp_obra.clear(); _mont_obra.clear()
        _medicoes_obra.clear(); _fin_obra.clear()
        st.rerun()

    # ── CARREGA TUDO ─────────────────────────────────────────
    jornada_an = _jornada_analise(oid_an)
    marcos_an  = _marcos_analise(oid_an)
    df_fab_an  = _fab_obra(oid_an)
    df_exp_an  = _exp_obra(oid_an)
    df_mont_an = _mont_obra(oid_an)
    df_med_an  = _medicoes_obra(oid_an)
    fin_an     = _fin_obra(oid_an)

    # ── CÁLCULO DE VOLUMES ───────────────────────────────────
    vol_proj_an = float(fin_an.get("volume_projeto") or fin_an.get("volume") or 0)
    vol_fab_an  = df_fab_an["volume_teorico"].sum()  if not df_fab_an.empty  else 0.0
    vol_exp_an  = df_exp_an["volume_real"].sum()     if not df_exp_an.empty  else 0.0
    vol_mont_an = df_mont_an["volume_teorico"].sum() if not df_mont_an.empty else 0.0

    fat_total_an = float(fin_an.get("faturamento_total") or 0)
    fat_real_an  = df_med_an["valor"].sum() if not df_med_an.empty else 0.0
    saldo_an     = fat_total_an - fat_real_an

    pct_fab_an  = (vol_fab_an  / vol_proj_an * 100) if vol_proj_an else 0.0
    pct_exp_an  = (vol_exp_an  / vol_proj_an * 100) if vol_proj_an else 0.0
    pct_mont_an = (vol_mont_an / vol_proj_an * 100) if vol_proj_an else 0.0
    pct_fat_an  = (fat_real_an / fat_total_an * 100) if fat_total_an else 0.0

    # ── CÁLCULO DA JORNADA ───────────────────────────────────
    etapas_an = {}
    total_ap_an = total_conc_an = imped_an = 0
    for item in jornada_an:
        if not item.get("aplicavel", True): continue
        total_ap_an += 1
        if item["status"] == "concluido":    total_conc_an += 1
        if item["status"] == "impedido":     imped_an += 1
        ne = item["template"]["etapa_nome"]
        etapas_an.setdefault(ne, []).append(item)
    pct_jornada_an = (total_conc_an / total_ap_an * 100) if total_ap_an else 0.0

    # ── PRAZO ────────────────────────────────────────────────
    entrega_pact_an = _safe_date(marcos_an.get("entrega_pact"))
    hoje_an = date.today()
    delta_prazo_an  = (entrega_pact_an - hoje_an).days if entrega_pact_an else None

    # ── BADGE DE SAÚDE ───────────────────────────────────────
    gap_an = abs(pct_fab_an - pct_fat_an)
    critico_an = (
        (delta_prazo_an is not None and delta_prazo_an < -15) or
        imped_an >= 2 or
        gap_an > 20
    )
    atencao_an = (
        (delta_prazo_an is not None and -15 <= delta_prazo_an < 0) or
        imped_an == 1
    )
    if critico_an:
        badge_cor_an, badge_txt_an = "🔴", "CRÍTICO"
    elif atencao_an:
        badge_cor_an, badge_txt_an = "🟡", "ATENÇÃO"
    else:
        badge_cor_an, badge_txt_an = "🟢", "SAUDÁVEL"

    # ════════════════════════════════════════════════════════
    # NÍVEL 1 — SAÚDE GERAL
    # ════════════════════════════════════════════════════════
    st.markdown(
        f"### {badge_cor_an} Saúde do Contrato: **{badge_txt_an}** &nbsp;&nbsp; "
        f"`{obra_an['cod4']}` — {obra_an['nome']}"
    )
    st.caption(
        f"Modalidade: **{obra_an.get('modalidade') or '—'}** &nbsp;|&nbsp; "
        f"Cliente: {obra_an.get('cliente') or '—'} &nbsp;|&nbsp; "
        f"Status: {obra_an.get('status') or '—'}"
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("🛤️ Jornada",
              f"{pct_jornada_an:.1f}%",
              f"{total_conc_an}/{total_ap_an} entregáveis")
    k2.metric("🏭 Físico (Fabricação)",
              f"{pct_fab_an:.1f}%" if vol_proj_an else "—",
              f"{vol_fab_an:,.1f} m³" if vol_proj_an else "Sem volume projeto")
    k3.metric("💰 Financeiro",
              f"{pct_fat_an:.1f}%" if fat_total_an else "—",
              fmt_brl(fat_real_an) if fat_total_an else "Sem contrato financeiro")
    prazo_lbl_an = (
        f"+{delta_prazo_an}d" if delta_prazo_an is not None and delta_prazo_an > 0
        else f"{delta_prazo_an}d" if delta_prazo_an is not None else "—"
    )
    prazo_det_an = (f"Entrega: {entrega_pact_an.strftime('%d/%m/%Y')}"
                    if entrega_pact_an else "Sem data pactuada")
    k4.metric("📅 Prazo", prazo_lbl_an, prazo_det_an)

    st.divider()

    # ════════════════════════════════════════════════════════
    # NÍVEL 2 — CARDS DA JORNADA
    # ════════════════════════════════════════════════════════
    st.markdown("#### 🛤️ Jornada do Contrato")

    if not jornada_an:
        st.info("Jornada não iniciada. Acesse **🛤️ Jornada da Obra** para iniciar.")
    else:
        _CORES_CARD = {
            "✅": ("#E8F5E9", "#43A047"),
            "🔴": ("#FFEBEE", "#E53935"),
            "🔵": ("#E3F2FD", "#1976D2"),
            "⬜": ("#F5F5F5", "#546E7A"),
        }
        n_et = len(etapas_an)
        cols_et = st.columns(n_et) if n_et > 0 else []
        for i, (nome_et, itens_et) in enumerate(etapas_an.items()):
            icone_et, texto_et = _derivar_etapa(itens_et)
            conc_et  = sum(1 for x in itens_et if x["status"] == "concluido")
            total_et = len(itens_et)
            bg, border = _CORES_CARD.get(icone_et, ("#F5F5F5", "#546E7A"))
            with cols_et[i]:
                st.markdown(
                    f"<div style='border-left:4px solid {border};background:{bg};"
                    f"padding:12px 8px;border-radius:6px;text-align:center;min-height:110px;'>"
                    f"<div style='font-size:1.4rem;'>{icone_et}</div>"
                    f"<div style='font-weight:600;font-size:0.82rem;margin:4px 0;'>{nome_et}</div>"
                    f"<div style='font-size:0.75rem;color:#555;'>{conc_et}/{total_et} itens</div>"
                    f"<div style='font-size:0.7rem;color:#888;margin-top:2px;'>{texto_et}</div>"
                    f"</div>",
                    unsafe_allow_html=True
                )

    st.divider()

    # ════════════════════════════════════════════════════════
    # NÍVEL 3 — VOLUMES & FINANCEIRO
    # ════════════════════════════════════════════════════════
    st.markdown("#### 📦 Volumes & Financeiro")

    v1, v2, v3, v4, v5, v6 = st.columns(6)
    with v1:
        st.metric("Vol. Projeto", f"{vol_proj_an:,.1f} m³" if vol_proj_an else "—")
        st.markdown("<div style='font-size:0.72rem;color:#888;'>referência contratual</div>",
                    unsafe_allow_html=True)
    with v2:
        st.metric("Fabricado", f"{vol_fab_an:,.1f} m³", f"{pct_fab_an:.1f}%")
        st.markdown(_barra_html(pct_fab_an, "#43A047"), unsafe_allow_html=True)
    with v3:
        st.metric("Expedido", f"{vol_exp_an:,.1f} m³", f"{pct_exp_an:.1f}%")
        st.markdown(_barra_html(pct_exp_an, "#FB8C00"), unsafe_allow_html=True)
    with v4:
        st.metric("Montado", f"{vol_mont_an:,.1f} m³", f"{pct_mont_an:.1f}%")
        st.markdown(_barra_html(pct_mont_an, "#7B1FA2"), unsafe_allow_html=True)
    with v5:
        st.metric("Faturado", fmt_brl(fat_real_an), f"{pct_fat_an:.1f}%")
        st.markdown(_barra_html(pct_fat_an, "#1976D2"), unsafe_allow_html=True)
    with v6:
        pct_saldo_an = abs((saldo_an / fat_total_an * 100)) if fat_total_an else 0
        cor_saldo    = "#E53935" if saldo_an < 0 else "#43A047"
        st.metric("Saldo Contrato", fmt_brl(saldo_an))
        st.markdown(_barra_html(pct_saldo_an, cor_saldo), unsafe_allow_html=True)

    st.divider()

    # ════════════════════════════════════════════════════════
    # NÍVEL 4 — GANTT (MARCOS)
    # ════════════════════════════════════════════════════════
    st.markdown("#### 📅 Cronograma — Pactuado vs Real")

    gantt_an = []

    def _add_gantt_an(nome, ini_p, fim_p, ini_r, fim_r):
        if ini_p and fim_p:
            gantt_an.append(dict(Etapa=nome, Tipo="Meta Pactuada",
                                 Inicio=ini_p, Fim=fim_p, Cor="#B0BEC5"))
        if ini_r:
            fim_c = fim_r if fim_r else hoje_an
            cor = "#43A047"
            if fim_p:
                if fim_r and fim_r > fim_p:         cor = "#E53935"
                elif not fim_r and hoje_an > fim_p: cor = "#E53935"
            tipo = "Execução Real" if fim_r else "Em andamento"
            gantt_an.append(dict(Etapa=nome, Tipo=tipo,
                                 Inicio=ini_r, Fim=fim_c, Cor=cor))

    _add_gantt_an("1. Fabricação",
                  _safe_date(marcos_an.get("inicio_fab_pact")),
                  _safe_date(marcos_an.get("fim_fab_pact")),
                  _safe_date(marcos_an.get("inicio_fab_real")),
                  _safe_date(marcos_an.get("fim_fab_real")))
    _add_gantt_an("2. Expedição",
                  _safe_date(marcos_an.get("inicio_exp_pact")),
                  _safe_date(marcos_an.get("fim_exp_pact")),
                  _safe_date(marcos_an.get("inicio_exp_real")),
                  _safe_date(marcos_an.get("fim_exp_real")))
    _add_gantt_an("3. Montagem",
                  _safe_date(marcos_an.get("inicio_mont_pact")),
                  _safe_date(marcos_an.get("entrega_pact")),
                  _safe_date(marcos_an.get("inicio_mont_real")),
                  _safe_date(marcos_an.get("entrega_real")))

    if not gantt_an:
        st.info("Marcos não cadastrados. Acesse **🛤️ Jornada da Obra** para preencher as datas.")
    else:
        df_g_an = pd.DataFrame(gantt_an)
        fig_g_an = px.timeline(
            df_g_an, x_start="Inicio", x_end="Fim", y="Etapa", color="Tipo",
            color_discrete_map={
                "Meta Pactuada": "#B0BEC5",
                "Execução Real": "#43A047",
                "Em andamento":  "#E53935",
            },
            barmode="group", height=220
        )
        fig_g_an.update_yaxes(autorange="reversed")
        fig_g_an.update_layout(
            xaxis=dict(title="", tickformat="%d/%b/%y"),
            yaxis=dict(title=""),
            legend=dict(orientation="h", yanchor="bottom", y=1.12, x=0, title=""),
            margin=dict(t=5, b=5, l=5, r=5),
        )
        for i_t, tr in enumerate(fig_g_an.data):
            cores_tr = df_g_an[df_g_an["Tipo"] == tr.name]["Cor"].tolist()
            fig_g_an.data[i_t].marker.color = cores_tr
        st.plotly_chart(fig_g_an, use_container_width=True)
        st.caption("Cinza: Planejado | Verde: Concluído no prazo | Vermelho: Atrasado / em atraso")

    st.divider()

    # ════════════════════════════════════════════════════════
    # NÍVEL 5 — PAINÉIS DE DETALHE
    # ════════════════════════════════════════════════════════
    st.markdown("#### 🔎 Detalhamento")
    p1_an, p2_an, p3_an = st.columns(3)

    # ── Painel 1: Pendências da Jornada ──────────────────────
    with p1_an:
        st.markdown("**📋 Jornada — Pendências e Impedimentos**")
        pendencias_an = [
            i for i in jornada_an
            if i.get("aplicavel", True) and i["status"] in ("em_andamento", "impedido")
        ]
        if not pendencias_an:
            st.caption("Nenhuma pendência ou impedimento ativo.")
        else:
            for item_p in pendencias_an[:10]:
                t_p = item_p["template"]
                icone_p = _MAP_ST_AN[item_p["status"]][0]
                desc_p  = t_p.get("descricao", "")
                cod_p   = t_p.get("item_cod", "")
                imp_p   = item_p.get("impedimento") or item_p.get("observacao") or ""
                st.markdown(
                    f"{icone_p} **{cod_p}** {desc_p[:45]}{'…' if len(desc_p) > 45 else ''}"
                )
                if imp_p:
                    st.caption(f"↳ {imp_p[:70]}")
            if len(pendencias_an) > 10:
                st.caption(f"… e mais {len(pendencias_an) - 10} itens")

    # ── Painel 2: Produção por Produto ───────────────────────
    with p2_an:
        st.markdown("**🏭 Produção por Produto**")
        if df_fab_an.empty:
            st.caption("Sem dados de fabricação para esta obra.")
        else:
            df_fab_an["produto"] = df_fab_an["produto"].fillna("—") if "produto" in df_fab_an.columns else "—"
            por_fab = df_fab_an.groupby("produto")["volume_teorico"].sum().reset_index()
            por_fab.columns = ["Produto", "Fab m³"]

            if not df_exp_an.empty and "produto" in df_exp_an.columns:
                df_exp_an["produto"] = df_exp_an["produto"].fillna("—")
                por_exp = df_exp_an.groupby("produto")["volume_real"].sum().reset_index()
                por_exp.columns = ["Produto", "Exp m³"]
                por_prod_an = por_fab.merge(por_exp, on="Produto", how="left").fillna(0)
            else:
                por_prod_an = por_fab.copy(); por_prod_an["Exp m³"] = 0.0

            if not df_mont_an.empty and "produto" in df_mont_an.columns:
                df_mont_an["produto"] = df_mont_an["produto"].fillna("—")
                por_mont = df_mont_an.groupby("produto")["volume_teorico"].sum().reset_index()
                por_mont.columns = ["Produto", "Mont m³"]
                por_prod_an = por_prod_an.merge(por_mont, on="Produto", how="left").fillna(0)
            else:
                por_prod_an["Mont m³"] = 0.0

            por_prod_an = por_prod_an.sort_values("Fab m³", ascending=False)
            st.dataframe(
                por_prod_an, use_container_width=True, hide_index=True,
                column_config={
                    "Fab m³":  st.column_config.NumberColumn(format="%.1f"),
                    "Exp m³":  st.column_config.NumberColumn(format="%.1f"),
                    "Mont m³": st.column_config.NumberColumn(format="%.1f"),
                }
            )

    # ── Painel 3: Medições Recentes ───────────────────────────
    with p3_an:
        st.markdown("**💰 Medições Recentes**")
        if df_med_an.empty:
            st.caption("Sem medições registradas para esta obra.")
        else:
            df_med_show = df_med_an.sort_values("data_emissao", ascending=False).head(8).copy()
            df_med_show["Valor"]  = df_med_show["valor"].apply(fmt_brl)
            df_med_show["Data"]   = df_med_show["data_emissao"].dt.strftime("%d/%m/%Y").fillna("—")
            df_med_show = df_med_show[["Data", "numero_nf", "tipo", "Valor"]]\
                .rename(columns={"numero_nf": "NF", "tipo": "Tipo"})
            st.dataframe(df_med_show, use_container_width=True, hide_index=True)
            st.caption(f"Total faturado: **{fmt_brl(fat_real_an)}** de **{fmt_brl(fat_total_an)}**")
# ==========================================================
# EDITAR OBRAS
# ==========================================================
elif pagina_selecionada == "✏️ Editar Obras":
    st.header("✏️ Editar Obras")
    st.caption("Edite os dados cadastrais e financeiros diretamente na tabela. "
               "Clique em **Salvar alterações** para persistir.")

    # ── loaders locais ────────────────────────────────────────
    @st.cache_data(ttl=30)
    def _ed_obras():
        r = supabase.table("obras")\
            .select("id, cod4, nome, status, modalidade, cliente")\
            .order("nome").execute()
        return pd.DataFrame(r.data or [])

    @st.cache_data(ttl=30)
    def _ed_fin():
        cols = ("cod4, faturamento_total, volume, volume_projeto, custo_total, lucro,"
                " responsavel, cnpj, razao_social, data_contrato")
        r = supabase.table("obras_financeiro").select(cols).execute()
        return pd.DataFrame(r.data or [])

    def _val_seguro(v):
        """Converte qualquer valor pandas/numpy para tipo JSON-seguro."""
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    def _str_seguro(v):
        """String limpa ou None — nunca 'nan'/'None' literais."""
        v2 = _val_seguro(v)
        if v2 is None:
            return None
        s = str(v2).strip()
        return None if s.lower() in ("nan", "nat", "none", "na", "") else s

    tab_obras, tab_fin = st.tabs(["🏗️ Obras", "💰 Financeiro"])

    # ═══════════════════════════════════════════════════════════
    # ABA 1 — OBRAS
    # ═══════════════════════════════════════════════════════════
    with tab_obras:
        # Bufferiza o DataFrame em session_state para que limpezas de cache
        # não resetem o data_editor enquanto o usuário ainda está editando.
        if "ed_obras_buf" not in st.session_state:
            st.session_state["ed_obras_buf"] = _ed_obras()

        df_ob = st.session_state["ed_obras_buf"]

        if df_ob.empty:
            st.info("Nenhuma obra cadastrada.")
        else:
            STATUS_OPT = ["A Iniciar","Em Andamento","Impedido","Concluído","N/A"]
            df_ob_edit = st.data_editor(
                df_ob,
                use_container_width=True,
                num_rows="fixed",
                hide_index=True,
                disabled=["id"],
                column_config={
                    "id":         st.column_config.NumberColumn("ID", width="small"),
                    "cod4":       st.column_config.TextColumn("Cód4", width="small",
                                      help="Código único de 4 dígitos da obra"),
                    "nome":       st.column_config.TextColumn("Nome da Obra", width="large"),
                    "status":     st.column_config.SelectboxColumn(
                                      "Status", options=STATUS_OPT, width="medium"),
                    "modalidade": st.column_config.TextColumn("Modalidade", width="medium"),
                    "cliente":    st.column_config.TextColumn("Cliente", width="medium"),
                },
                key="editor_obras",
            )
            col_sav1, col_inf1 = st.columns([1, 4])
            with col_sav1:
                salvar_obras = st.button("💾 Salvar alterações", type="primary",
                                         key="btn_salvar_obras")
            if salvar_obras:
                erros, ok = [], 0
                for _, row in df_ob_edit.iterrows():
                    try:
                        payload_o = {
                            "cod4":       _str_seguro(row.get("cod4")),
                            "nome":       _str_seguro(row.get("nome")),
                            "status":     _str_seguro(row.get("status")),
                            "modalidade": _str_seguro(row.get("modalidade")),
                            "cliente":    _str_seguro(row.get("cliente")),
                        }
                        # Remove campos None para não sobrescrever com NULL
                        payload_o = {k: v for k, v in payload_o.items() if v is not None}
                        if payload_o:
                            supabase.table("obras").update(payload_o)\
                                .eq("id", int(row["id"])).execute()
                        ok += 1
                    except Exception as _ex:
                        erros.append(f"ID {row['id']}: {_ex}")
                # Limpa caches e recarrega buffer com dados frescos do banco
                try: _ed_obras.clear()
                except: pass
                try: carregar_obras_completo.clear()
                except: pass
                try: carregar_obras_ativas.clear()
                except: pass
                # Atualiza o buffer e reseta o widget do data_editor
                st.session_state["ed_obras_buf"] = _ed_obras()
                if "editor_obras" in st.session_state:
                    del st.session_state["editor_obras"]
                if erros:
                    st.error(f"❌ {len(erros)} erro(s):\n" + "\n".join(erros))
                else:
                    st.success(f"✅ {ok} obra(s) atualizadas!")
                    st.rerun()

    # ═══════════════════════════════════════════════════════════
    # ABA 2 — FINANCEIRO
    # ═══════════════════════════════════════════════════════════
    with tab_fin:
        df_fin = _ed_fin()
        if df_fin.empty:
            st.info("Nenhum registro financeiro cadastrado.")
        else:
            # Mostrar também o nome da obra para contexto
            df_ob2 = _ed_obras()[["cod4","nome"]].rename(columns={"nome":"obra_nome"})
            # Drop obra_nome from df_fin if already present (evita coluna duplicada)
            df_fin_m = df_fin.drop(columns=["obra_nome"], errors="ignore")
            df_fin_show = df_fin_m.merge(df_ob2, on="cod4", how="left")
            # data_contrato precisa ser datetime para DateColumn funcionar
            if "data_contrato" in df_fin_show.columns:
                df_fin_show["data_contrato"] = pd.to_datetime(
                    df_fin_show["data_contrato"], errors="coerce"
                )
            # Reordenar: nome primeiro
            cols_show = ["obra_nome","cod4","faturamento_total","volume","volume_projeto",
                         "custo_total","lucro","responsavel","cnpj","razao_social","data_contrato"]
            cols_show = [c for c in cols_show if c in df_fin_show.columns]
            df_fin_edit = st.data_editor(
                df_fin_show[cols_show],
                use_container_width=True,
                num_rows="fixed",
                hide_index=True,
                disabled=["cod4","obra_nome"],
                column_config={
                    "obra_nome":        st.column_config.TextColumn("Obra", width="large"),
                    "cod4":             st.column_config.TextColumn("Cód4", width="small"),
                    "faturamento_total":st.column_config.NumberColumn("Fat. Total (R$)",
                                            format="R$ %.2f", width="medium"),
                    "volume":           st.column_config.NumberColumn("Volume (m³)",
                                            format="%.1f", width="small"),
                    "volume_projeto":   st.column_config.NumberColumn("Vol. Projeto (m³)",
                                            format="%.1f", width="small"),
                    "custo_total":      st.column_config.NumberColumn("Custo Total (R$)",
                                            format="R$ %.2f", width="medium"),
                    "lucro":            st.column_config.NumberColumn("Lucro (R$)",
                                            format="R$ %.2f", width="medium"),
                    "responsavel":      st.column_config.TextColumn("Responsável", width="medium"),
                    "cnpj":             st.column_config.TextColumn("CNPJ", width="medium"),
                    "razao_social":     st.column_config.TextColumn("Razão Social", width="large"),
                    "data_contrato":    st.column_config.DateColumn("Data Contrato",
                                            format="DD/MM/YYYY", width="medium"),
                },
                key="editor_fin",
            )
            col_sav2, _ = st.columns([1, 4])
            with col_sav2:
                salvar_fin = st.button("💾 Salvar alterações", type="primary",
                                        key="btn_salvar_fin")
            if salvar_fin:
                erros2, ok2 = [], 0
                for _, row in df_fin_edit.iterrows():
                    cod4_key = row.get("cod4")
                    if not cod4_key:
                        continue
                    try:
                        payload_fin = {}
                        for campo in ["faturamento_total","volume","volume_projeto",
                                      "custo_total","lucro"]:
                            v = row.get(campo)
                            payload_fin[campo] = (float(v)
                                                  if v is not None and str(v) not in ("","nan")
                                                  else None)
                        for campo in ["responsavel","cnpj","razao_social"]:
                            v = row.get(campo)
                            payload_fin[campo] = (str(v).strip()
                                                  if v is not None and str(v).strip() not in ("","nan","None")
                                                  else None)
                        # data_contrato: converter para isoformat
                        dc = row.get("data_contrato")
                        if dc is not None and str(dc) not in ("","nan","None","NaT"):
                            try:
                                payload_fin["data_contrato"] = pd.to_datetime(dc).date().isoformat()
                            except Exception:
                                payload_fin["data_contrato"] = None
                        else:
                            payload_fin["data_contrato"] = None

                        # upsert via cod4
                        r_chk = (supabase.table("obras_financeiro")
                                 .select("cod4").eq("cod4", cod4_key).execute())
                        if r_chk.data:
                            supabase.table("obras_financeiro").update(payload_fin)\
                                .eq("cod4", cod4_key).execute()
                        else:
                            payload_fin["cod4"] = cod4_key
                            supabase.table("obras_financeiro").insert(payload_fin).execute()
                        ok2 += 1
                    except Exception as _ex2:
                        erros2.append(f"cod4 {cod4_key}: {_ex2}")
                try: _ed_fin.clear()
                except: pass
                try: carregar_obras_completo.clear()
                except: pass
                if erros2:
                    st.error(f"❌ {len(erros2)} erro(s):\n" + "\n".join(erros2))
                else:
                    st.success(f"✅ {ok2} obra(s) atualizadas no financeiro!")

        # ── Mapa de correlação entre tabelas ─────────────────────────────────
        with st.expander("🔗 Mapa de correlação entre tabelas do banco", expanded=False):
            st.markdown("""
| Tabela | Chave | Conecta com |
|--------|-------|-------------|
| `obras` | `id`, `cod4` | **tabela mestre** |
| `obras_financeiro` | `cod4` | `obras.cod4` |
| `obras_tarefas` | `obra_id` | `obras.id` |
| `producao_fabricacao` | `obra_id` + `cod4_obra` | `obras.id` / `obras.cod4` |
| `producao_transporte` | `obra_id` + `cod4_obra` | `obras.id` / `obras.cod4` |
| `producao_montagem` | `obra_id` + `cod4_obra` | `obras.id` / `obras.cod4` |
| `medicoes` | `obra_id` | `obras.id` |
| `custos` | — | via `centro_custos` (4 dígitos iniciais) |
| `folha` | `obra_id` | `obras.id` |

**Chave universal:** `cod4` (4 dígitos numéricos) — presente em todas as tabelas, diretamente ou como prefixo de texto.

Para cruzar **custos** com obras nos dashboards: `SUBSTRING(centro_custos, 1, 4)` = `obras.cod4`.
            """)

# ==========================================================
# FOLHA / RH
# ==========================================================
elif pagina_selecionada == "👷 Folha / RH":
    st.header("👷 Folha / RH")

    # ── FUNÇÕES DE CARGA ──────────────────────────────────────────────────────

    @st.cache_data(ttl=600)
    def carregar_folha_meses():
        resp = supabase.table("folha").select("mes").execute()
        if not resp.data:
            return []
        meses = sorted(set(r["mes"][:7] for r in resp.data if r.get("mes")), reverse=True)
        return meses

    @st.cache_data(ttl=600)
    def carregar_folha_mes(mes_iso):
        rows, page, size = [], 0, 1000
        q = (supabase.table("folha").select("*")
             .gte("mes", mes_iso + "-01")
             .lte("mes", mes_iso + "-31"))
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size:
                break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        cols_num = [
            "proventos", "base_fgts", "base_inss", "he_50", "he_70", "he_80",
            "he_100", "he_110", "he_150", "adc_noturno", "dsr", "desconto_compart",
            "vale_transporte", "alimentacao", "seguro_vida", "assistencia_medica",
            "soma_hes", "proventos_13", "base_fgts_13", "desc_1_13",
            "fgts_art22", "valor_funcionario"
        ]
        for c in cols_num:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        return df

    @st.cache_data(ttl=600)
    def carregar_folha_historico():
        df = fetch_all("folha",
                       "mes, situacao, proventos, valor_funcionario, soma_hes, "
                       "vale_transporte, alimentacao, seguro_vida, assistencia_medica")
        if df.empty:
            return df
        for c in ["proventos", "valor_funcionario", "soma_hes",
                  "vale_transporte", "alimentacao", "seguro_vida", "assistencia_medica"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        df["mes_ref"] = df["mes"].apply(lambda v: str(v)[:7] if v else None)
        return df

    # ── CARREGA MESES DISPONÍVEIS ─────────────────────────────────────────────
    meses_disp = carregar_folha_meses()

    if not meses_disp:
        st.warning("Nenhuma folha importada ainda. Use o **📥 Importador de Arquivos** → Rota 8 para importar.")
        st.stop()

    # ── FILTROS GLOBAIS ───────────────────────────────────────────────────────
    fh1, fh2, fh3 = st.columns([2, 2, 1])

    mes_labels = {m: m[5:7] + "/" + m[:4] for m in meses_disp}
    mes_sel = fh1.selectbox(
        "Mês de referência",
        options=meses_disp,
        format_func=lambda m: mes_labels[m],
        key="folha_mes_sel",
        label_visibility="collapsed"
    )

    sit_opcoes = ["Ativos (A)", "Férias (F)", "Demitidos (D)", "Todos"]
    sit_sel = fh2.selectbox("Situação", sit_opcoes, key="folha_sit_sel", label_visibility="collapsed")

    if fh3.button("🔄 Atualizar", key="btn_folha_refresh"):
        limpar_todos_caches()
        st.rerun()

    # ── CARREGA DADOS DO MÊS ─────────────────────────────────────────────────
    with st.spinner("Carregando folha..."):
        df_folha = carregar_folha_mes(mes_sel)

    if df_folha.empty:
        st.warning("Sem dados para este mês.")
        st.stop()

    # Aplica filtro de situação
    sit_map = {"Ativos (A)": "A", "Férias (F)": "F", "Demitidos (D)": "D"}
    if sit_sel != "Todos":
        df_f = df_folha[df_folha["situacao"] == sit_map[sit_sel]].copy()
    else:
        df_f = df_folha.copy()

    # ── CARDS KPI ────────────────────────────────────────────────────────────
    ativos_n    = len(df_folha[df_folha["situacao"] == "A"])
    ferias_n    = len(df_folha[df_folha["situacao"] == "F"])
    demit_n     = len(df_folha[df_folha["situacao"] == "D"])
    folha_total = df_folha[df_folha["situacao"].isin(["A", "F"])]["valor_funcionario"].sum()
    prov_total  = df_folha[df_folha["situacao"].isin(["A", "F"])]["proventos"].sum()
    he_total    = df_folha[df_folha["situacao"].isin(["A", "F"])]["soma_hes"].fillna(0).sum()
    custo_medio = folha_total / max(ativos_n + ferias_n, 1)
    pct_he      = (he_total / prov_total * 100) if prov_total > 0 else 0.0
    encargos_tot = folha_total - prov_total

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("👥 Colaboradores", f"{ativos_n + ferias_n}", f"🚪 {demit_n} desligado(s)")
    k2.metric("🏖️ Em Férias", str(ferias_n))
    k3.metric("💰 Folha Total", fmt_brl(folha_total),
              help="Soma de Valor do Funcionário (Ativos + Férias)")
    k4.metric("📊 Custo Médio", fmt_brl(custo_medio))
    k5.metric("⏱️ Horas Extras", fmt_brl(he_total),
              delta=f"{pct_he:.1f}% dos proventos", delta_color="inverse")
    k6.metric("📎 Encargos", fmt_brl(encargos_tot),
              help="Folha Total − Proventos brutos")

    st.divider()

    # ── ABAS ─────────────────────────────────────────────────────────────────
    tab_vis, tab_evo, tab_det = st.tabs(["📊 Visão do Mês", "📈 Evolução Mensal", "📋 Detalhamento"])

    # ══════════════════════════════════════════════════════════════════════════
    # ABA 1 — VISÃO DO MÊS
    # ══════════════════════════════════════════════════════════════════════════
    with tab_vis:
        col_esq, col_dir = st.columns(2)

        with col_esq:
            st.markdown("#### 👷 Headcount por Função")
            df_func = (
                df_f[df_f["situacao"].isin(["A", "F"])]
                .groupby("funcao")
                .size()
                .reset_index(name="qtd")
                .sort_values("qtd", ascending=True)
                .tail(15)
            )
            if not df_func.empty:
                fig_func = go.Figure(go.Bar(
                    x=df_func["qtd"],
                    y=df_func["funcao"],
                    orientation="h",
                    marker_color="#1976D2",
                    hovertemplate="%{y}<br>%{x} colaboradores<extra></extra>"
                ))
                fig_func.update_layout(
                    xaxis=dict(title="Colaboradores"),
                    yaxis=dict(autorange="reversed"),
                    margin=dict(t=10, b=10, r=10),
                    height=420
                )
                st.plotly_chart(fig_func, use_container_width=True, key="chart_func")
            else:
                st.info("Sem dados para o filtro selecionado.")

        with col_dir:
            st.markdown("#### 💰 Composição do Custo Total")
            df_comp = df_folha[df_folha["situacao"].isin(["A", "F"])]
            vt_tot   = df_comp["vale_transporte"].sum()
            ali_tot  = df_comp["alimentacao"].sum()
            seg_tot  = df_comp["seguro_vida"].sum()
            med_tot  = df_comp["assistencia_medica"].sum()
            he_comp  = df_comp["soma_hes"].fillna(0).sum()
            sal_base = df_comp["proventos"].sum() - he_comp
            enc_comp = folha_total - prov_total

            labels_c = ["Salário Base", "Horas Extras", "Encargos", "VT", "Alimentação", "Saúde/Seguro"]
            values_c = [sal_base, he_comp, enc_comp, vt_tot, ali_tot, seg_tot + med_tot]
            cores_c  = ["#1976D2", "#E53935", "#FB8C00", "#43A047", "#8E24AA", "#00ACC1"]
            pairs = [(l, v, c) for l, v, c in zip(labels_c, values_c, cores_c) if v > 0]
            if pairs:
                fig_comp = go.Figure(go.Pie(
                    labels=[p[0] for p in pairs],
                    values=[p[1] for p in pairs],
                    hole=0.42,
                    marker_colors=[p[2] for p in pairs],
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>R$ %{value:,.0f}<br>%{percent}<extra></extra>"
                ))
                fig_comp.update_layout(
                    showlegend=False,
                    margin=dict(t=10, b=10, l=10, r=10),
                    height=420
                )
                st.plotly_chart(fig_comp, use_container_width=True, key="chart_comp")

        st.divider()

        st.markdown("#### 📋 Custo por Função")
        df_custo_func = (
            df_folha[df_folha["situacao"].isin(["A", "F"])]
            .groupby("funcao")
            .agg(
                headcount=("nome_colaborador", "count"),
                proventos=("proventos", "sum"),
                he=("soma_hes", "sum"),
                custo_total=("valor_funcionario", "sum"),
            )
            .reset_index()
            .sort_values("custo_total", ascending=False)
        )
        df_custo_func["custo_medio"] = df_custo_func["custo_total"] / df_custo_func["headcount"]
        df_custo_func["pct_folha"]   = df_custo_func["custo_total"] / folha_total * 100

        st.dataframe(
            df_custo_func.rename(columns={
                "funcao":      "Função",
                "headcount":   "Qtd",
                "proventos":   "Proventos (R$)",
                "he":          "HE (R$)",
                "custo_total": "Custo Total (R$)",
                "custo_medio": "Custo Médio (R$)",
                "pct_folha":   "% Folha",
            }),
            use_container_width=True,
            hide_index=True,
            height=min(500, 36 + 35 * len(df_custo_func)),
            column_config={
                "Proventos (R$)":   st.column_config.NumberColumn(format="R$ %.0f"),
                "HE (R$)":          st.column_config.NumberColumn(format="R$ %.0f"),
                "Custo Total (R$)": st.column_config.NumberColumn(format="R$ %.0f"),
                "Custo Médio (R$)": st.column_config.NumberColumn(format="R$ %.0f"),
                "% Folha":          st.column_config.ProgressColumn(
                                        "% Folha", min_value=0, max_value=100, format="%.1f%%"),
            }
        )

    # ══════════════════════════════════════════════════════════════════════════
    # ABA 2 — EVOLUÇÃO MENSAL
    # ══════════════════════════════════════════════════════════════════════════
    with tab_evo:
        with st.spinner("Carregando histórico..."):
            df_hist = carregar_folha_historico()

        if df_hist.empty:
            st.info("Sem histórico disponível.")
        else:
            df_hist_at = df_hist[df_hist["situacao"].isin(["A", "F"])].copy()
            evo = (
                df_hist_at.groupby("mes_ref")
                .agg(
                    headcount=("valor_funcionario", "count"),
                    folha=("valor_funcionario", "sum"),
                    proventos=("proventos", "sum"),
                    he=("soma_hes", "sum"),
                )
                .reset_index()
                .sort_values("mes_ref")
            )
            evo["custo_medio"] = evo["folha"] / evo["headcount"]
            evo["pct_he"]      = evo["he"] / evo["proventos"] * 100

            st.markdown("#### 📈 Evolução da Folha Total e Headcount")
            fig_evo = go.Figure()
            fig_evo.add_trace(go.Bar(
                x=evo["mes_ref"], y=evo["folha"],
                name="Folha Total",
                marker_color="#1976D2",
                hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"
            ))
            fig_evo.add_trace(go.Scatter(
                x=evo["mes_ref"], y=evo["headcount"],
                name="Headcount",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="#E53935", width=2),
                hovertemplate="Headcount: %{y}<extra></extra>"
            ))
            fig_evo.update_layout(
                yaxis=dict(title="R$", tickformat=",.0f"),
                yaxis2=dict(title="Colaboradores", overlaying="y", side="right", showgrid=False),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=50, b=20),
                height=320,
                hovermode="x unified"
            )
            st.plotly_chart(fig_evo, use_container_width=True, key="chart_evo_folha")

            st.divider()

            st.markdown("#### ⏱️ Evolução das Horas Extras (% dos Proventos)")
            fig_he = go.Figure()
            fig_he.add_trace(go.Bar(
                x=evo["mes_ref"], y=evo["he"],
                name="HE (R$)",
                marker_color="#FB8C00",
                hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"
            ))
            fig_he.add_trace(go.Scatter(
                x=evo["mes_ref"], y=evo["pct_he"],
                name="% sobre Proventos",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="#E53935", width=2, dash="dot"),
                hovertemplate="%{y:.1f}%<extra></extra>"
            ))
            fig_he.update_layout(
                yaxis=dict(title="R$", tickformat=",.0f"),
                yaxis2=dict(title="%", overlaying="y", side="right",
                            showgrid=False, ticksuffix="%"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=50, b=20),
                height=300,
                hovermode="x unified"
            )
            st.plotly_chart(fig_he, use_container_width=True, key="chart_he_evo")

            st.divider()

            st.markdown("#### 📋 Tabela Resumo Mensal")
            evo_show = evo.copy()
            evo_show["Folha"]       = evo_show["folha"].apply(fmt_brl)
            evo_show["HE"]          = evo_show["he"].apply(fmt_brl)
            evo_show["Custo Médio"] = evo_show["custo_medio"].apply(fmt_brl)
            evo_show["% HE"]        = evo_show["pct_he"].apply(lambda v: f"{v:.1f}%")
            st.dataframe(
                evo_show[["mes_ref", "headcount", "Folha", "HE", "% HE", "Custo Médio"]]
                    .rename(columns={"mes_ref": "Mês", "headcount": "Headcount"})
                    .sort_values("Mês", ascending=False),
                use_container_width=True,
                hide_index=True
            )

    # ══════════════════════════════════════════════════════════════════════════
    # ABA 3 — DETALHAMENTO INDIVIDUAL
    # ══════════════════════════════════════════════════════════════════════════
    with tab_det:
        st.markdown(f"#### 📋 Colaboradores — {mes_labels[mes_sel]}")

        fd1, fd2 = st.columns(2)
        funcoes_disp = sorted(df_f["funcao"].dropna().unique())
        filtro_func = fd1.selectbox(
            "Filtrar por função", ["Todas"] + funcoes_disp,
            key="folha_det_func", label_visibility="collapsed"
        )
        texto_busca = fd2.text_input(
            "Buscar nome", placeholder="Digite parte do nome...",
            key="folha_busca", label_visibility="collapsed"
        )

        df_det = df_f.copy()
        if filtro_func != "Todas":
            df_det = df_det[df_det["funcao"] == filtro_func]
        if texto_busca:
            df_det = df_det[
                df_det["nome_colaborador"].str.upper().str.contains(
                    texto_busca.upper(), na=False
                )
            ]

        colunas_det = [
            "nome_colaborador", "funcao", "situacao",
            "proventos", "soma_hes", "vale_transporte",
            "alimentacao", "seguro_vida", "assistencia_medica",
            "valor_funcionario"
        ]
        colunas_det_ok = [c for c in colunas_det if c in df_det.columns]
        df_det_show = df_det[colunas_det_ok].sort_values("valor_funcionario", ascending=False)

        st.caption(f"**{len(df_det_show)}** colaborador(es) · ordenado por custo total")
        st.dataframe(
            df_det_show.rename(columns={
                "nome_colaborador":  "Nome",
                "funcao":            "Função",
                "situacao":          "Sit.",
                "proventos":         "Proventos (R$)",
                "soma_hes":          "HE (R$)",
                "vale_transporte":   "VT (R$)",
                "alimentacao":       "Alim. (R$)",
                "seguro_vida":       "Seguro (R$)",
                "assistencia_medica":"Saúde (R$)",
                "valor_funcionario": "Custo Total (R$)",
            }),
            use_container_width=True,
            hide_index=True,
            height=min(600, 36 + 35 * len(df_det_show)),
            column_config={
                "Proventos (R$)":    st.column_config.NumberColumn(format="R$ %.2f"),
                "HE (R$)":           st.column_config.NumberColumn(format="R$ %.2f"),
                "VT (R$)":           st.column_config.NumberColumn(format="R$ %.2f"),
                "Alim. (R$)":        st.column_config.NumberColumn(format="R$ %.2f"),
                "Seguro (R$)":       st.column_config.NumberColumn(format="R$ %.2f"),
                "Saúde (R$)":        st.column_config.NumberColumn(format="R$ %.2f"),
                "Custo Total (R$)":  st.column_config.NumberColumn(format="R$ %.2f"),
            }
        )

        if len(df_det_show) > 0:
            st.caption(
                f"**Totais do filtro** → "
                f"Proventos: {fmt_brl(df_det_show['proventos'].sum())} | "
                f"HE: {fmt_brl(df_det_show['soma_hes'].sum())} | "
                f"Custo Total: {fmt_brl(df_det_show['valor_funcionario'].sum())}"
            )

# ==========================================================
# CUSTOS & DESPESAS
# ==========================================================
elif pagina_selecionada == "💸 Custos & Despesas":

    # ── Cores fixas por grupo ────────────────────────────────
    _COR_GRUPO = {
        "Pessoal":                  "#1976D2",
        "Insumos Estruturais":      "#E53935",
        "Materiais de Consumo":     "#FB8C00",
        "Serviços PJ":              "#43A047",
        "Logística":                "#7B1FA2",
        "Benefícios":               "#00ACC1",
        "Peças e Manutenção":       "#F57F17",
        "Energia e Infraestrutura": "#546E7A",
        "Outros":                   "#9E9E9E",
    }
    _COR_TIPO = {
        "Direto (Obra)":      "#1976D2",
        "Indireto (Fábrica)": "#43A047",
        "Equipamento":        "#FB8C00",
        "Outros":             "#9E9E9E",
    }
    _COR_TRIB = {
        "Previdência (INSS)":        "#1976D2",
        "FGTS":                      "#43A047",
        "IR/IRRF":                   "#E53935",
        "Contribuições Sociais":     "#FB8C00",
        "Taxas e Licenças":          "#7B1FA2",
        "Previdência Social (GPS)":  "#00ACC1",
        "Outros Tributos":           "#9E9E9E",
    }

    # ── Mapeamento conta_gerencial → grupo_custo ─────────────
    _MAP_GRUPO = {
        "CIMENTO":                              "Insumos Estruturais",
        "AÇO PARA ESTRUTURAS":                  "Insumos Estruturais",
        "CORDOALHA":                            "Insumos Estruturais",
        "AREIA NATURAL":                        "Insumos Estruturais",
        "PEDRAS BRITADAS":                      "Insumos Estruturais",
        "ADITIVO":                              "Insumos Estruturais",
        "METACAULIM":                           "Insumos Estruturais",
        "MINÉRIO NÃO REAGIDO":                  "Insumos Estruturais",
        "MATERIAIS DE CONSUMO":                 "Materiais de Consumo",
        "PEÇAS E ACESSÓRIOS":                   "Peças e Manutenção",
        "FERRAMENTAS E UTENSÍLIOS":             "Peças e Manutenção",
        "PEÇAS DE DESGASTE":                    "Peças e Manutenção",
        "GASES E ELETRODOS":                    "Peças e Manutenção",
        "LUBRIFICANTES":                        "Peças e Manutenção",
        "PNEUS E CÂMARAS":                      "Peças e Manutenção",
        "SERVIÇOS DE MANUTENÇÃO DE EQUIP. PJ":  "Peças e Manutenção",
        "SALARIOS":                             "Pessoal",
        "INSS S/ FOLHA":                        "Pessoal",
        "INSS FOLHA":                           "Pessoal",
        "FGTS":                                 "Pessoal",
        "HORAS EXTRAS":                         "Pessoal",
        "FERIAS":                               "Pessoal",
        "DECIMO TERCEIRO  SALARIO":             "Pessoal",
        "ADICIONAL NOTURNO":                    "Pessoal",
        "ADICIONAL DE PERICULOSIDADE":          "Pessoal",
        "13 SALARIO INDENIZADO":                "Pessoal",
        "FERIAS INDENIZADAS":                   "Pessoal",
        "AVISO PREVIO INDENIZADO":              "Pessoal",
        "FGTS RESCISÃO":                        "Pessoal",
        "PRODUTIVIDADE":                        "Pessoal",
        "1/3 FERIAS":                           "Pessoal",
        "ABONO PECUNIARIO DE FERIAS":           "Pessoal",
        "1/3 ABONO PECUNIARIO DE FERIAS":       "Pessoal",
        "ANUENIO":                              "Pessoal",
        "MULTA ART 480":                        "Pessoal",
        "ALIMENTAÇÃO":                          "Benefícios",
        "ALIMENTACAO":                          "Benefícios",
        "ASSISTÊNCIA MÉDICA":                   "Benefícios",
        "ASSISTENCIA MEDICA/ODONTOLOGICA":      "Benefícios",
        "SEGURO DE VIDA":                       "Benefícios",
        "VALE-TRANSPORTE":                      "Benefícios",
        "TRANSPORTE DE FUNCIONARIOS":           "Benefícios",
        "CESTAS BASICAS":                       "Benefícios",
        "EXAMES OCUPACIONAIS":                  "Benefícios",
        "FARDAMENTO E EQUIPAMENTO DE SEGURANÇA": "Benefícios",
        "ESTAGIARIOS":                          "Benefícios",
        "OUTROS SERVIÇOS PJ":                   "Serviços PJ",
        "SERVIÇOS PJ PARA PRODUÇÃO":            "Serviços PJ",
        "LOCAÇÕES PJ":                          "Serviços PJ",
        "SERVIÇOS TÉCNICOS":                    "Serviços PJ",
        "SERVIÇOS DE CONSULTORIAS PJ":          "Serviços PJ",
        "SERVIÇOS DE DESENV./SUPORTE DE SISTEMAS": "Serviços PJ",
        "SERVIÇOS DE CONSERVAÇÃO E LIMPEZA PJ": "Serviços PJ",
        "FRETE PJ":                             "Logística",
        "FRETE PJ PARA CLIENTES":               "Logística",
        "COMBUSTÍVEIS":                         "Logística",
        "PEDÁGIOS/ESTACIONAMENTOS/CONDUÇÕES":   "Logística",
        "ENERGIA ELÉTRICA":                     "Energia e Infraestrutura",
        "DEPRECIAÇÕES":                         "Energia e Infraestrutura",
        "DEPRECIACOES":                         "Energia e Infraestrutura",
        "MATERIAIS APLICADOS EM REFORMAS":      "Energia e Infraestrutura",
    }

    def _enriquecer_df(df):
        """Adiciona colunas derivadas: mes, ano, cod4, tipo_centro, grupo_custo.
        Totalmente vetorizado — sem apply() linha a linha.
        valor_global = valor original com sinal (estornos ficam negativos)
        valor_abs    = abs() para gráficos e somas"""
        df = df.copy()
        df["data"]         = pd.to_datetime(df["data"], errors="coerce")
        df["valor_global"] = pd.to_numeric(df["valor_global"], errors="coerce")
        df["valor_abs"]    = df["valor_global"].abs()
        df["mes"]          = df["data"].dt.to_period("M").astype(str)
        df["ano"]          = df["data"].dt.year.astype(str)

        # cod4: extrai 4 primeiros dígitos do centro_custos via regex vetorizado
        cc = df["centro_custos"].fillna("").astype(str).str.strip()
        df["cod4"] = cc.str.extract(r"^(\d{4})", expand=False)

        # tipo_centro: vetorizado com np.select (ordem de prioridade)
        cc_up = cc.str.upper()
        cond_direto = df["cod4"].notna()
        cond_equip  = cc_up.str.contains(
            "CAMINHÃO|CAMINHAO|PÓRTICO|PORTICO|RETROESCAVADEIRA|"
            "BETONEIRA|MUNCK|MÁQUINA|MAQUINA|CENTRAL DE CONCRETO",
            regex=True, na=False)
        cond_indir  = cc_up.str.contains(
            "PRODUÇÃO|PRODUCAO|LABORATÓRIO|LABORATORIO|MONTAGEM|MANUTENÇÃO|MANUTENCAO",
            regex=True, na=False)
        df["tipo_centro"] = np.select(
            [cond_direto, cond_equip, cond_indir],
            ["Direto (Obra)", "Equipamento", "Indireto (Fábrica)"],
            default="Outros"
        )

        # grupo_custo: map direto pelo dicionário (O(n) hash lookup, sem loop Python)
        df["grupo_custo"] = (df["conta_gerencial"]
                             .fillna("").astype(str)
                             .str.upper().str.strip()
                             .map(_MAP_GRUPO)
                             .fillna("Outros"))
        return df

    # ── Colunas necessárias (evita SELECT * em tabelas grandes) ──
    _COLS_CUSTOS = ("data, id_lancamento, numero_doc, centro_custos, conta_macro,"
                    " conta_gerencial, cli_fornecedor, produto_servico, criado_por,"
                    " valor_global, qtd, preco_unitario, origem, chave_coligada, cod_tipo_doc")

    # ── Funções de carga ─────────────────────────────────────
    @st.cache_data(ttl=600)
    def carregar_receitas_dashboard():
        df = fetch_all("receitas", "*")
        if df.empty:
            return df
        # Normaliza coluna de data (pode ser "data" ou "data_emissao")
        if "data" not in df.columns and "data_emissao" in df.columns:
            df["data"] = df["data_emissao"]
        df["data"]         = pd.to_datetime(df.get("data"), errors="coerce")
        df["valor_global"] = pd.to_numeric(df.get("valor_global"), errors="coerce")
        df["mes"]          = df["data"].dt.to_period("M").astype(str)
        df["ano"]          = df["data"].dt.year.astype(str)
        cc = df.get("centro_custos", pd.Series(dtype=str)).fillna("").astype(str)
        df["cod4"] = cc.str.extract(r"^(\d{4})", expand=False)
        return df

    @st.cache_data(ttl=600)
    def carregar_medicoes_faturamento():
        df = fetch_all("medicoes", "obra_id, data_emissao, tipo, valor, descricao")
        if df.empty:
            return df
        df = df[~df["tipo"].isin(["NOTA FISCAL CANCELADA", "NOTA FISCAL REMESSA"])]
        df["valor"]        = pd.to_numeric(df["valor"], errors="coerce")
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
        df["mes"]          = df["data_emissao"].dt.to_period("M").astype(str)
        return df

    @st.cache_data(ttl=300)
    def carregar_producao_mensal():
        df = pd.DataFrame(
            supabase.table("mv_fabricacao_mensal").select("mes, vol_teorico").execute().data or []
        )
        if df.empty:
            return pd.Series(dtype=float)
        df["vol_teorico"] = pd.to_numeric(df["vol_teorico"], errors="coerce").fillna(0)
        return df.groupby("mes")["vol_teorico"].sum()

    def _limpar_caches_custos():
        limpar_todos_caches()

    # ── Cabeçalho ────────────────────────────────────────────
    h1, h2 = st.columns([8, 1])
    h1.header("💸 Custos & Despesas")
    if h2.button("🔄", key="cd_refresh", help="Atualizar dados"):
        _limpar_caches_custos()
        st.rerun()

    # ── Carga dos dados ──────────────────────────────────────
    with st.spinner("Carregando dados..."):
        df_all = carregar_custos_completo()
        df_rec = carregar_receitas_dashboard()

    if df_all.empty:
        st.warning("Nenhum dado encontrado. Importe os arquivos pelas Rotas 7 e 10.")
        st.stop()

    # Separa para compatibilidade com abas que filtram por origem
    df_cus = df_all[df_all["origem"] == "Custo"].copy()
    df_des = df_all[df_all["origem"] == "Despesa"].copy()

    st.caption(
        f"📦 Fonte (view): **{len(df_cus)}** Custos · "
        f"**{len(df_des)}** Despesas · "
        f"**{len(df_rec)}** Receitas"
    )

    # ── Filtros globais ──────────────────────────────────────
    flt1, flt2, flt3, flt4, flt5 = st.columns([2, 2, 2, 2, 1])

    with flt1:
        _tipo_periodo = st.radio("Período", ["Predefinido", "Personalizado"],
                                 horizontal=True, key="cd_tipo_periodo")
        if _tipo_periodo == "Predefinido":
            _pred = st.selectbox("", ["Últimos 6 meses", "Últimos 12 meses",
                                      "Últimos 24 meses", "Todo o período"],
                                 index=1, key="cd_pred_periodo",
                                 label_visibility="collapsed")
            _hoje = date.today()
            if _pred == "Últimos 6 meses":
                _dt_ini = _hoje - timedelta(days=182)
            elif _pred == "Últimos 12 meses":
                _dt_ini = _hoje - timedelta(days=365)
            elif _pred == "Últimos 24 meses":
                _dt_ini = _hoje - timedelta(days=730)
            else:
                _dt_ini = None
            _dt_fim = _hoje
        else:
            _intervalo = st.date_input("Intervalo", value=(), key="cd_intervalo")
            _dt_ini = _intervalo[0] if len(_intervalo) > 0 else None
            _dt_fim = _intervalo[1] if len(_intervalo) > 1 else None

    with flt2:
        _origens_sel = st.multiselect("Origem", ["Custo", "Despesa"],
                                      default=["Custo", "Despesa"], key="cd_origem")

    with flt3:
        _centros_disp = sorted(df_all["centro_custos"].dropna().unique().tolist())
        _centros_sel  = st.multiselect("Centro de Custos", _centros_disp,
                                       default=[], key="cd_centro_custos",
                                       placeholder="Todos os centros")

    with flt4:
        _macros_disp = sorted(df_all["conta_macro"].dropna().unique().tolist())
        _contas_macro_sel = st.multiselect("Conta Macro", _macros_disp,
                                           default=[], key="cd_conta_macro",
                                           placeholder="Todas")

    with flt5:
        st.write("")  # espaçamento

    # Aplicar filtros
    df_fil = df_all.copy()
    if _dt_ini:
        df_fil = df_fil[df_fil["data"] >= pd.Timestamp(_dt_ini)]
    if _dt_fim:
        df_fil = df_fil[df_fil["data"] <= pd.Timestamp(_dt_fim)]
    if _origens_sel:
        df_fil = df_fil[df_fil["origem"].isin(_origens_sel)]
    if _centros_sel:
        df_fil = df_fil[df_fil["centro_custos"].isin(_centros_sel)]
    if _contas_macro_sel:
        df_fil = df_fil[df_fil["conta_macro"].isin(_contas_macro_sel)]

    if df_fil.empty:
        st.warning("Nenhum registro encontrado com os filtros aplicados.")
        st.stop()

    # ── KPIs globais ─────────────────────────────────────────
    _tot      = df_fil["valor_abs"].sum()
    _tot_cus  = df_fil.loc[df_fil["origem"] == "Custo",   "valor_abs"].sum()
    _tot_des  = df_fil.loc[df_fil["origem"] == "Despesa", "valor_abs"].sum()
    _n_lanc   = len(df_fil)
    _ticket   = _tot / _n_lanc if _n_lanc > 0 else 0

    kc1, kc2, kc3, kc4, kc5 = st.columns(5)
    kc1.metric("💰 Total do Período",   fmt_brl(_tot))
    kc2.metric("🏗️ Total Custos",       fmt_brl(_tot_cus))
    kc3.metric("📋 Total Despesas",      fmt_brl(_tot_des))
    kc4.metric("🔢 Nº de Lançamentos",  f"{_n_lanc:,}".replace(",", "."))
    kc5.metric("🎯 Ticket Médio",        fmt_brl(_ticket))

    # ── Abas ─────────────────────────────────────────────────
    aba1, aba2, aba3, aba4, aba5, aba6, aba7 = st.tabs([
        "📊 Visão Geral",
        "📈 Curva ABC",
        "🔍 Padrão & Anomalias",
        "🔄 Faturamento Direto",
        "📋 Lançamentos",
        "📐 Custo por m³",
        "⚖️ Tributos",
    ])

    # ── Componente drill-through ─────────────────────────────
    def _drill_through(df, titulo, prefixo="dt"):
        st.divider()
        st.markdown(f"#### 🔍 Drill-Through: {titulo}")
        dc1, dc2, dc3 = st.columns(3)
        _k = f"cd_{prefixo}_{titulo[:8]}"
        _busca_for = dc1.text_input("Buscar fornecedor", key=f"{_k}_for")
        _contas    = ["Todas"] + sorted(df["conta_gerencial"].dropna().unique().tolist())
        _fil_cg    = dc2.selectbox("Conta Gerencial", _contas, key=f"{_k}_cg")
        _meses_dt  = ["Todos"] + sorted(df["mes"].dropna().unique().tolist(), reverse=True)
        _fil_mes   = dc3.selectbox("Mês", _meses_dt, key=f"{_k}_mes")

        df_dt = df.copy()
        if _busca_for:
            df_dt = df_dt[df_dt["cli_fornecedor"].fillna("").str.upper()
                          .str.contains(_busca_for.upper(), na=False)]
        if _fil_cg != "Todas":
            df_dt = df_dt[df_dt["conta_gerencial"] == _fil_cg]
        if _fil_mes != "Todos":
            df_dt = df_dt[df_dt["mes"] == _fil_mes]

        # Mostra valor_global original (com sinal) no drill-through
        _cols_dt = ["data","centro_custos","conta_gerencial","cli_fornecedor",
                    "produto_servico","criado_por","valor_global"]
        _cols_dt_ok = [c for c in _cols_dt if c in df_dt.columns]
        df_dt_show = df_dt[_cols_dt_ok].rename(columns={
            "data":            "Data",
            "centro_custos":   "Centro de Custo",
            "conta_gerencial": "Conta Gerencial",
            "cli_fornecedor":  "Fornecedor",
            "produto_servico": "Produto/Serviço",
            "criado_por":      "Criado Por",
            "valor_global":    "Valor (R$)",
        })
        st.dataframe(df_dt_show, use_container_width=True, hide_index=True,
                     height=max(300, min(600, 36 + 35 * len(df_dt_show))))
        st.caption(f"**{len(df_dt_show)}** lançamentos · Total: {fmt_brl(df_dt['valor_abs'].sum() if 'valor_abs' in df_dt.columns else df_dt['valor_global'].abs().sum())}")

    # ════════════════════════════════════════════════════════
    # ABA 1 — Visão Geral
    # ════════════════════════════════════════════════════════
    with aba1:
        # Filtros internos da aba
        vg_f1, vg_f2 = st.columns(2)
        _vg_grupos = vg_f1.multiselect(
            "Grupo de Custo", list(_COR_GRUPO.keys()),
            default=list(_COR_GRUPO.keys()), key="cd_vg_grupo"
        )
        _vg_tipos = vg_f2.multiselect(
            "Tipo de Centro",
            ["Direto (Obra)", "Indireto (Fábrica)", "Equipamento", "Outros"],
            default=["Direto (Obra)", "Indireto (Fábrica)", "Equipamento", "Outros"],
            key="cd_vg_tipo"
        )
        df_vg = df_fil.copy()
        if _vg_grupos:
            df_vg = df_vg[df_vg["grupo_custo"].isin(_vg_grupos)]
        if _vg_tipos:
            df_vg = df_vg[df_vg["tipo_centro"].isin(_vg_tipos)]

        # Bloco A — Evolução mensal empilhada
        st.subheader("Evolução Mensal por Grupo")
        _ev = (df_vg.groupby(["mes","grupo_custo"])["valor_abs"]
               .sum().reset_index())
        _ev_tot = df_vg.groupby("mes")["valor_abs"].sum().reset_index()
        _ev_tot.columns = ["mes","total"]

        _meses_ord = sorted(_ev["mes"].unique())
        fig_ev = go.Figure()
        for _grp, _cor in _COR_GRUPO.items():
            _d = _ev[_ev["grupo_custo"] == _grp]
            if _d.empty:
                continue
            _d = (_d.set_index("mes")[["valor_abs"]]
                  .reindex(_meses_ord, fill_value=0).reset_index())
            fig_ev.add_trace(go.Bar(
                x=_d["mes"], y=_d["valor_abs"],
                name=_grp, marker_color=_cor,
                hovertemplate=f"<b>{_grp}</b><br>%{{x}}<br>R$ %{{y:,.0f}}<extra></extra>"
            ))
        _ev_tot_ord = _ev_tot.set_index("mes")[["total"]].reindex(_meses_ord, fill_value=0).reset_index()
        fig_ev.add_trace(go.Scatter(
            x=_ev_tot_ord["mes"], y=_ev_tot_ord["total"],
            name="Total", mode="lines+markers",
            line=dict(color="#212121", width=2, dash="dot"),
            yaxis="y2",
            hovertemplate="<b>Total</b><br>%{x}<br>R$ %{y:,.0f}<extra></extra>"
        ))
        fig_ev.update_layout(
            barmode="stack", hovermode="x unified", height=380,
            yaxis=dict(title="R$"),
            yaxis2=dict(title="Total", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=40, b=40)
        )
        st.plotly_chart(fig_ev, use_container_width=True)

        # Bloco B — Mapa de calor de sazonalidade
        st.subheader("Sazonalidade Mensal")
        _heat = df_vg.copy()
        _heat["mes_num"] = _heat["data"].dt.month
        _heat["ano_str"] = _heat["data"].dt.year.astype(str)
        _pivot = _heat.pivot_table(index="ano_str", columns="mes_num",
                                   values="valor_abs", aggfunc="sum")
        _meses_pt = {1:"Jan",2:"Fev",3:"Mar",4:"Abr",5:"Mai",6:"Jun",
                     7:"Jul",8:"Ago",9:"Set",10:"Out",11:"Nov",12:"Dez"}
        _pivot.columns = [_meses_pt.get(c, str(c)) for c in _pivot.columns]
        _n_anos = len(_pivot)
        fig_heat = px.imshow(
            _pivot, text_auto=".2s",
            color_continuous_scale=["#43A047","#FFEE58","#E53935"],
            aspect="auto"
        )
        fig_heat.update_layout(height=max(180, min(70 * _n_anos, 500)),
                                margin=dict(t=20, b=20))
        st.plotly_chart(fig_heat, use_container_width=True)

        # Bloco C — Donuts
        st.subheader("Composição")
        dc1, dc2 = st.columns(2)
        with dc1:
            _grp_vals = df_vg.groupby("grupo_custo")["valor_abs"].sum()
            fig_d1 = go.Figure(go.Pie(
                labels=_grp_vals.index, values=_grp_vals.values,
                hole=0.42,
                marker_colors=[_COR_GRUPO.get(g, "#9E9E9E") for g in _grp_vals.index],
                textinfo="label+percent"
            ))
            fig_d1.update_layout(title="Por Grupo de Custo", height=350,
                                  margin=dict(t=40, b=10))
            st.plotly_chart(fig_d1, use_container_width=True)
        with dc2:
            _tc_vals = df_vg.groupby("tipo_centro")["valor_abs"].sum()
            fig_d2 = go.Figure(go.Pie(
                labels=_tc_vals.index, values=_tc_vals.values,
                hole=0.42,
                marker_colors=[_COR_TIPO.get(g, "#9E9E9E") for g in _tc_vals.index],
                textinfo="label+percent"
            ))
            fig_d2.update_layout(title="Por Tipo de Centro", height=350,
                                  margin=dict(t=40, b=10))
            st.plotly_chart(fig_d2, use_container_width=True)

        # Bloco D — Top 20 Centros de Custo
        st.subheader("Distribuição por Centro de Custos (Top 20)")
        _cc_top = (df_vg.groupby("centro_custos")["valor_abs"].sum()
                   .reset_index()
                   .rename(columns={"centro_custos": "centro", "valor_abs": "valor"})
                   .sort_values("valor", ascending=False).head(20))
        if not _cc_top.empty:
            _cc_top["tipo_centro"] = _cc_top["centro"].apply(
                lambda cc: (
                    "Direto (Obra)"      if re.match(r"^\d{4}", str(cc)) else
                    "Equipamento"        if any(k in str(cc).upper() for k in
                        ["CAMINHÃO","CAMINHAO","PÓRTICO","PORTICO","BETONEIRA","MUNCK","MÁQUINA","MAQUINA"]) else
                    "Indireto (Fábrica)" if any(k in str(cc).upper() for k in
                        ["PRODUÇÃO","PRODUCAO","LABORATÓRIO","LABORATORIO","MONTAGEM","MANUTENÇÃO","MANUTENCAO"]) else
                    "Outros"
                )
            )
            fig_cc = go.Figure(go.Bar(
                x=_cc_top["valor"], y=_cc_top["centro"],
                orientation="h",
                marker_color=[_COR_TIPO.get(t, "#9E9E9E") for t in _cc_top["tipo_centro"]],
                hovertemplate="<b>%{y}</b><br>R$ %{x:,.0f}<extra></extra>"
            ))
            fig_cc.update_layout(
                height=500, hovermode="y unified",
                xaxis_title="Valor (R$)",
                yaxis=dict(autorange="reversed"),
                margin=dict(t=20, b=40)
            )
            st.plotly_chart(fig_cc, use_container_width=True)

    # ════════════════════════════════════════════════════════
    # ABA 2 — Curva ABC
    # ════════════════════════════════════════════════════════
    with aba2:
        abc_r1, abc_r2 = st.columns([3, 1])
        _dim_abc = abc_r1.radio(
            "Dimensão de análise",
            ["Por Fornecedor (Cli/For)", "Por Conta Gerencial", "Por Centro de Custo",
             "Por Conta Macro", "Por Produto/Serviço"],
            horizontal=True, key="cd_dim_abc"
        )
        _col_abc = {
            "Por Fornecedor (Cli/For)": "cli_fornecedor",
            "Por Conta Gerencial":      "conta_gerencial",
            "Por Centro de Custo":      "centro_custos",
            "Por Conta Macro":          "conta_macro",
            "Por Produto/Serviço":      "produto_servico",
        }[_dim_abc]
        _abc_orig = abc_r2.radio("Origem", ["Todos","Custo","Despesa"],
                                  horizontal=True, key="cd_abc_origem")

        _df_abc_base = df_fil.copy()
        if _abc_orig != "Todos":
            _df_abc_base = _df_abc_base[_df_abc_base["origem"] == _abc_orig]

        _abc = (_df_abc_base.groupby(_col_abc, dropna=False)["valor_abs"]
                .sum().reset_index()
                .rename(columns={_col_abc: "nome", "valor_abs": "valor"})
                .sort_values("valor", ascending=False).reset_index(drop=True))
        _tot_abc = _abc["valor"].sum()
        _abc["pct_individual"] = _abc["valor"] / _tot_abc * 100
        _abc["pct_acumulado"]  = _abc["pct_individual"].cumsum()
        _abc["classe_abc"]     = _abc["pct_acumulado"].apply(
            lambda x: "A" if x <= 80 else ("B" if x <= 95 else "C"))
        _abc["Rank"] = range(1, len(_abc) + 1)
        _abc_top = _abc.head(30)

        # KPIs
        _n_a   = (_abc["classe_abc"] == "A").sum()
        _pct_a = _abc.loc[_abc["classe_abc"] == "A", "valor"].sum() / _tot_abc * 100 if _tot_abc else 0
        _top1  = _abc.iloc[0] if len(_abc) > 0 else None
        _top5_pct = _abc.head(5)["valor"].sum() / _tot_abc * 100 if _tot_abc else 0

        ka1, ka2, ka3, ka4 = st.columns(4)
        ka1.metric(f"Itens Classe A", str(_n_a))
        ka2.metric("% total na classe A", f"{_pct_a:.1f}%")
        if _top1 is not None:
            ka3.metric("Maior item", f"{str(_top1['nome'])[:25]}",
                       delta=f"{_top1['pct_individual']:.1f}% do total")
        ka4.metric("Concentração Top-5", f"{_top5_pct:.1f}%")

        # Gráfico ABC
        _cores_abc = {"A":"#E53935","B":"#FB8C00","C":"#43A047"}
        fig_abc = go.Figure()
        fig_abc.add_trace(go.Bar(
            x=_abc_top["valor"], y=_abc_top["nome"],
            orientation="h",
            marker_color=[_cores_abc.get(c,"#9E9E9E") for c in _abc_top["classe_abc"]],
            name="Valor",
            hovertemplate="<b>%{y}</b><br>R$ %{x:,.0f}<extra></extra>"
        ))
        fig_abc.add_trace(go.Scatter(
            x=_abc_top["pct_acumulado"], y=_abc_top["nome"],
            mode="lines+markers", name="% Acumulado",
            line=dict(color="#616161", dash="dash"),
            xaxis="x2",
            hovertemplate="%{y}<br>%{x:.1f}%<extra></extra>"
        ))
        fig_abc.add_hline(y=None, secondary_y=False)
        fig_abc.update_layout(
            height=500, hovermode="y unified",
            xaxis=dict(title="Valor (R$)"),
            xaxis2=dict(title="% Acumulado", overlaying="x", side="top",
                        range=[0,100], showgrid=False),
            yaxis=dict(autorange="reversed"),
            margin=dict(t=50, b=40)
        )
        fig_abc.add_vline(x=80, line_dash="dot", line_color="#E53935",
                          annotation_text="80% (A)", xref="x2")
        fig_abc.add_vline(x=95, line_dash="dot", line_color="#FB8C00",
                          annotation_text="95% (B)", xref="x2")
        st.plotly_chart(fig_abc, use_container_width=True)

        # Tabela ABC
        st.subheader("Tabela Completa")
        _abc_show = _abc.rename(columns={
            "nome":"Nome","valor":"Valor (R$)",
            "pct_individual":"% Individual","pct_acumulado":"% Acumulado",
            "classe_abc":"Classe"
        })[["Rank","Nome","Valor (R$)","% Individual","% Acumulado","Classe"]]
        _sel_abc = st.dataframe(
            _abc_show, use_container_width=True, hide_index=True,
            on_select="rerun", selection_mode="single-row",
            column_config={
                "Valor (R$)":    st.column_config.NumberColumn(format="R$ %.0f"),
                "% Individual":  st.column_config.NumberColumn(format="%.1f%%"),
                "% Acumulado":   st.column_config.NumberColumn(format="%.1f%%"),
            }
        )
        _sel_rows_abc = _sel_abc.selection.rows if hasattr(_sel_abc, "selection") else []
        if _sel_rows_abc:
            _item_sel = _abc.iloc[_sel_rows_abc[0]]["nome"]
            st.session_state["custos_drill_item"]    = _item_sel
            st.session_state["custos_drill_dimensao"] = _col_abc
            _df_drill = _df_abc_base[_df_abc_base[_col_abc].fillna("").astype(str) == str(_item_sel)]
            _drill_through(_df_drill, str(_item_sel), prefixo="abc")

    # ════════════════════════════════════════════════════════
    # ABA 3 — Padrão & Anomalias
    # ════════════════════════════════════════════════════════
    with aba3:
        # Bloco A — Detector de anomalias
        _anom_dim_map = {
            "Conta Gerencial":  "conta_gerencial",
            "Centro de Custos": "centro_custos",
            "Conta Macro":      "conta_macro",
        }
        _anom_dim_lbl = st.radio("Dimensão de análise", list(_anom_dim_map.keys()),
                                  horizontal=True, key="cd_anom_dim")
        _col_anom = _anom_dim_map[_anom_dim_lbl]
        st.subheader(f"Detector de Anomalias por {_anom_dim_lbl}")
        _anom_rows = []
        _mes_rec = df_fil["mes"].max()
        for _item_a in df_fil[_col_anom].dropna().unique():
            _sub = df_fil[df_fil[_col_anom] == _item_a].groupby("mes")["valor_abs"].sum()
            if len(_sub) < 2:
                continue
            _med = _sub.mean()
            _std = _sub.std()
            _ult = _sub.get(_mes_rec, None)
            if _ult is None or _std == 0:
                continue
            _sig = (_ult - _med) / _std
            _status = ("🔴 Anomalia" if abs(_sig) > 2 else
                       "🟡 Atenção" if abs(_sig) > 1 else "🟢 Normal")
            _anom_rows.append({
                _anom_dim_lbl:      _item_a,
                "Média Mensal (R$)": _med,
                "Último Mês (R$)":  _ult,
                "Δ vs Média":       _ult - _med,
                "Desvio (σ)":       round(_sig, 2),
                "Status":           _status,
                "_abs_sig":         abs(_sig),
                "_abs_delta":       abs(_ult - _med),
            })
        if _anom_rows:
            _df_anom = (pd.DataFrame(_anom_rows)
                        .sort_values("_abs_sig", ascending=False)
                        .drop(columns=["_abs_sig","_abs_delta"]))
            st.dataframe(_df_anom, use_container_width=True, hide_index=True,
                         column_config={
                             "Média Mensal (R$)": st.column_config.NumberColumn(format="R$ %.0f"),
                             "Último Mês (R$)":   st.column_config.NumberColumn(format="R$ %.0f"),
                             "Δ vs Média":         st.column_config.NumberColumn(format="R$ %.0f"),
                         })

            # Maiores variações absolutas (Top 10 em R$)
            st.subheader("Maiores Variações Absolutas (Top 10 em R$)")
            _df_var10 = (pd.DataFrame(_anom_rows)
                         .sort_values("_abs_delta", ascending=False)
                         .head(10)
                         .drop(columns=["_abs_sig","_abs_delta"])
                         [[_anom_dim_lbl, "Último Mês (R$)", "Média Mensal (R$)", "Δ vs Média", "Status"]])
            st.dataframe(_df_var10, use_container_width=True, hide_index=True,
                         column_config={
                             "Último Mês (R$)":   st.column_config.NumberColumn(format="R$ %.0f"),
                             "Média Mensal (R$)": st.column_config.NumberColumn(format="R$ %.0f"),
                             "Δ vs Média":         st.column_config.NumberColumn(format="R$ %.0f"),
                         })
        else:
            st.info("Dados insuficientes para análise de anomalias.")

        # Bloco B — Dispersão
        st.subheader("Dispersão de Lançamentos Individuais")
        _p95 = df_fil["valor_abs"].quantile(0.95)
        _df_disp = df_fil.copy()

        fig_disp = go.Figure()
        for _grp, _cor in _COR_GRUPO.items():
            _dd = _df_disp[(_df_disp["grupo_custo"] == _grp) & (_df_disp["valor_abs"] < _p95)]
            if _dd.empty:
                continue
            fig_disp.add_trace(go.Scatter(
                x=_dd["data"], y=_dd["valor_abs"],
                mode="markers", name=_grp,
                marker=dict(color=_cor, size=5, opacity=0.6),
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "Conta: %{customdata[1]}<br>"
                    "Forn.: %{customdata[2]}<br>"
                    "CC: %{customdata[3]}<br>"
                    "Valor: R$ %{y:,.0f}<extra></extra>"
                ),
                customdata=_dd[["cli_fornecedor","conta_gerencial","cli_fornecedor","centro_custos"]].fillna("—").values
            ))
        _dd_out = _df_disp[_df_disp["valor_abs"] >= _p95]
        if not _dd_out.empty:
            fig_disp.add_trace(go.Scatter(
                x=_dd_out["data"], y=_dd_out["valor_abs"],
                mode="markers", name="≥ P95",
                marker=dict(color="#E53935", size=12, symbol="diamond"),
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "Conta: %{customdata[1]}<br>"
                    "Valor: R$ %{y:,.0f}<extra></extra>"
                ),
                customdata=_dd_out[["cli_fornecedor","conta_gerencial"]].fillna("—").values
            ))
        fig_disp.update_layout(height=380, hovermode="closest",
                                xaxis_title="Data", yaxis_title="Valor (R$)",
                                margin=dict(t=20, b=40))
        st.plotly_chart(fig_disp, use_container_width=True)
        st.caption(f"Pontos vermelhos grandes = lançamentos acima do percentil 95 (acima de {fmt_brl(_p95)})")

        # Bloco C — Comparativo mês a mês
        st.subheader("Comparativo Mês a Mês")
        _comp_dim_map = {
            "Conta Gerencial":       "conta_gerencial",
            "Centro de Custos":      "centro_custos",
            "Conta Macro":           "conta_macro",
            "Fornecedor (Cli/For)":  "cli_fornecedor",
        }
        _comp_dim_lbl = st.radio("Agrupar por", list(_comp_dim_map.keys()),
                                  horizontal=True, key="cd_comp_dim")
        _col_comp = _comp_dim_map[_comp_dim_lbl]

        _meses_disp = sorted(df_fil["mes"].dropna().unique(), reverse=True)
        if len(_meses_disp) >= 2:
            cc1, cc2 = st.columns(2)
            _mes_a = cc1.selectbox("Mês A", _meses_disp, index=0, key="cd_mes_a")
            _mes_b = cc2.selectbox("Mês B", _meses_disp, index=1, key="cd_mes_b")
            _df_ma = (df_fil[df_fil["mes"] == _mes_a]
                      .groupby(_col_comp)["valor_abs"].sum()
                      .reset_index().rename(columns={"valor_abs":"val_a"}))
            _df_mb = (df_fil[df_fil["mes"] == _mes_b]
                      .groupby(_col_comp)["valor_abs"].sum()
                      .reset_index().rename(columns={"valor_abs":"val_b"}))
            _df_comp = _df_ma.merge(_df_mb, on=_col_comp, how="outer").fillna(0)
            _df_comp["Δ Valor"]   = _df_comp["val_a"] - _df_comp["val_b"]
            _df_comp["Δ %"]       = (_df_comp["Δ Valor"] / _df_comp["val_b"].replace(0, float("nan")) * 100).fillna(0)
            _df_comp = _df_comp.sort_values("val_a", ascending=False)
            st.dataframe(
                _df_comp.rename(columns={
                    _col_comp: _comp_dim_lbl,
                    "val_a": f"Mês {_mes_a} (R$)",
                    "val_b": f"Mês {_mes_b} (R$)",
                }),
                use_container_width=True, hide_index=True,
                column_config={
                    f"Mês {_mes_a} (R$)": st.column_config.NumberColumn(format="R$ %.0f"),
                    f"Mês {_mes_b} (R$)": st.column_config.NumberColumn(format="R$ %.0f"),
                    "Δ Valor":             st.column_config.NumberColumn(format="R$ %.0f"),
                    "Δ %":                 st.column_config.NumberColumn(format="%.1f%%"),
                }
            )
        else:
            st.info("São necessários pelo menos 2 meses de dados para o comparativo.")

    # ════════════════════════════════════════════════════════
    # ABA 4 — Faturamento Direto
    # ════════════════════════════════════════════════════════
    with aba4:
        st.info(
            "Esta aba analisa exclusivamente o **faturamento direto** — medições emitidas "
            "por obra — confrontado com os **custos de insumos da fábrica** (Cimento, Aço, "
            "Cordoalha, Frete). O objetivo é calcular a **margem de repasse** dos insumos "
            "e identificar obras em que o custo supera o faturado."
        )

        with st.spinner("Carregando medições..."):
            df_med_fat = carregar_medicoes_faturamento()

        # Aplicar filtro de período
        if not df_med_fat.empty:
            if _dt_ini:
                df_med_fat = df_med_fat[
                    df_med_fat["data_emissao"] >= pd.Timestamp(_dt_ini)]
            if _dt_fim:
                df_med_fat = df_med_fat[
                    df_med_fat["data_emissao"] <= pd.Timestamp(_dt_fim)]

        # ── Insumos fábrica (custos diretos de produção) ─────
        _palavras_fab = ["CIMENTO","AÇO","ACO","CORDOALHA","FRETE","TRANSPORTE"]
        _mask_ins = df_fil["conta_gerencial"].fillna("").str.upper().apply(
            lambda x: any(p in x for p in _palavras_fab)
        )
        df_ins = df_fil[_mask_ins].copy()

        # ── KPIs ─────────────────────────────────────────────
        _fat_total  = df_med_fat["valor"].sum() if not df_med_fat.empty else 0
        _cus_ins    = df_ins["valor_abs"].sum()
        _margem_rep = _fat_total - _cus_ins
        _pct_fat    = (_cus_ins / _fat_total * 100) if _fat_total > 0 else 0

        kfd1, kfd2, kfd3, kfd4 = st.columns(4)
        kfd1.metric("💵 Faturamento Direto",    fmt_brl(_fat_total))
        kfd2.metric("🏭 Custo Insumos Fábrica", fmt_brl(_cus_ins))
        kfd3.metric("📈 Margem de Repasse",      fmt_brl(_margem_rep),
                    delta=f"{_margem_rep:+,.0f}".replace(",", "."))
        kfd4.metric("⚖️ % do Faturamento Total",
                    f"{_pct_fat:.1f}%",
                    help="Parcela do faturamento consumida pelos insumos de fábrica")

        st.divider()

        # ── Confronto por Tipo de Insumo ─────────────────────
        st.markdown("#### 🧱 Confronto por Tipo de Insumo")

        _MAP_INSUMO = {
            "CIMENTO":     "Cimento",
            "AÇO":         "Aço",
            "ACO":         "Aço",
            "CORDOALHA":   "Cordoalha",
            "FRETE":       "Frete",
            "TRANSPORTE":  "Frete",
        }

        def _detectar_insumo(conta):
            cu = str(conta).upper().strip()
            for k, v in _MAP_INSUMO.items():
                if k in cu:
                    return v
            return "Outros"

        df_ins["tipo_insumo"] = df_ins["conta_gerencial"].apply(_detectar_insumo)

        _tbl_ins = (
            df_ins.groupby("tipo_insumo", as_index=False)["valor_abs"]
            .sum()
            .rename(columns={"tipo_insumo": "Insumo", "valor_abs": "Custo (R$)"})
            .sort_values("Custo (R$)", ascending=False)
        )
        _tbl_ins["% do Total"] = (
            _tbl_ins["Custo (R$)"] / _tbl_ins["Custo (R$)"].sum() * 100
        ).round(1)

        st.dataframe(
            _tbl_ins,
            use_container_width=True, hide_index=True,
            column_config={
                "Custo (R$)":  st.column_config.NumberColumn(format="R$ %.0f"),
                "% do Total":  st.column_config.NumberColumn(format="%.1f%%"),
            }
        )

        st.divider()

        # ── Custo vs Faturamento por Obra ────────────────────
        st.markdown("#### 🏗️ Custo de Insumos vs Faturamento por Obra")

        if df_med_fat.empty:
            st.info("Sem dados de medições para o período selecionado.")
        else:
            # Faturamento por obra_id
            _fat_obra = (
                df_med_fat.groupby("obra_id", as_index=False)["valor"]
                .sum()
                .rename(columns={"valor": "Faturamento"})
            )

            # Custo insumos: usa cod4 como proxy de obra_id
            _cus_obra = (
                df_ins.groupby("cod4", as_index=False)["valor_abs"]
                .sum()
                .rename(columns={"cod4": "obra_cod4", "valor_abs": "Custo Insumos"})
            )

            # Lookup obras para nome
            _obras_raw = carregar_obras_ativas()
            _obras_lkp = {str(o.get("id", "")): o.get("nome", str(o.get("id", "")))
                          for o in _obras_raw} if _obras_raw else {}

            _fat_obra["Obra"] = _fat_obra["obra_id"].astype(str).map(
                lambda x: _obras_lkp.get(x, f"Obra {x}")
            )

            _df_bar = _fat_obra[["Obra", "Faturamento"]].copy()

            # Tenta cruzar custo por cod4 (best-effort)
            _fat_obra["cod4_str"] = _fat_obra["obra_id"].astype(str).apply(
                lambda x: x.zfill(4) if x.isdigit() else x
            )
            _cus_obra["obra_cod4"] = _cus_obra["obra_cod4"].astype(str)
            _merge = _fat_obra.merge(
                _cus_obra, left_on="cod4_str", right_on="obra_cod4", how="left"
            )
            _merge["Custo Insumos"] = _merge["Custo Insumos"].fillna(0)

            _df_bar = _merge[["Obra", "Faturamento", "Custo Insumos"]].copy()
            _df_bar = _df_bar[_df_bar["Faturamento"] > 0].sort_values(
                "Faturamento", ascending=False
            ).head(20)

            if _df_bar.empty:
                st.info("Nenhuma obra com faturamento no período.")
            else:
                _fig_bar = go.Figure()
                _fig_bar.add_trace(go.Bar(
                    name="Faturamento",
                    x=_df_bar["Obra"],
                    y=_df_bar["Faturamento"],
                    marker_color="#1976D2",
                ))
                _fig_bar.add_trace(go.Bar(
                    name="Custo Insumos",
                    x=_df_bar["Obra"],
                    y=_df_bar["Custo Insumos"],
                    marker_color="#E53935",
                ))
                _fig_bar.update_layout(
                    barmode="group",
                    height=420,
                    margin=dict(t=20, b=100),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    xaxis_tickangle=-35,
                    yaxis_tickformat=",.0f",
                )
                st.plotly_chart(_fig_bar, use_container_width=True)

                st.caption(
                    f"Mostrando top {len(_df_bar)} obras por faturamento · "
                    f"Custo insumos cruzado por código de obra (cod4)"
                )

    # ════════════════════════════════════════════════════════
    # ABA 5 — Lançamentos
    # ════════════════════════════════════════════════════════
    with aba5:
        st.markdown("#### 📋 Lançamentos do período")
        lc1, lc2, lc3, lc4 = st.columns(4)
        _busca_for4 = lc1.text_input("Fornecedor", key="cd_lanc_for",
                                      label_visibility="collapsed",
                                      placeholder="Buscar fornecedor...")
        _fil_cc4 = lc2.selectbox("Centro de Custo",
                      ["Todos"] + sorted(df_fil["centro_custos"].dropna().unique().tolist()),
                      key="cd_lanc_cc", label_visibility="collapsed")
        _fil_cm4 = lc3.selectbox("Conta Macro",
                      ["Todas"] + sorted(df_fil["conta_macro"].dropna().unique().tolist()),
                      key="cd_lanc_cm", label_visibility="collapsed")
        _fil_cg4 = lc4.selectbox("Conta Gerencial",
                      ["Todas"] + sorted(df_fil["conta_gerencial"].dropna().unique().tolist()),
                      key="cd_lanc_cg", label_visibility="collapsed")

        df_lanc = df_fil.copy()
        if _busca_for4:
            df_lanc = df_lanc[
                df_lanc["cli_fornecedor"].fillna("").str.upper()
                .str.contains(_busca_for4.upper(), na=False)]
        if _fil_cc4 != "Todos":
            df_lanc = df_lanc[df_lanc["centro_custos"] == _fil_cc4]
        if _fil_cm4 != "Todas":
            df_lanc = df_lanc[df_lanc["conta_macro"] == _fil_cm4]
        if _fil_cg4 != "Todas":
            df_lanc = df_lanc[df_lanc["conta_gerencial"] == _fil_cg4]

        _cols_l = ["data","origem","centro_custos","conta_macro",
                   "conta_gerencial","cli_fornecedor","produto_servico","valor_global"]
        _cols_l_ok = [c for c in _cols_l if c in df_lanc.columns]
        st.dataframe(
            df_lanc[_cols_l_ok].rename(columns={
                "data":            "Data",
                "origem":          "Origem",
                "centro_custos":   "Centro de Custo",
                "conta_macro":     "Conta Macro",
                "conta_gerencial": "Conta Gerencial",
                "cli_fornecedor":  "Fornecedor",
                "produto_servico": "Produto/Serviço",
                "valor_global":    "Valor (R$)",
            }).sort_values("Data", ascending=False),
            use_container_width=True, hide_index=True,
            height=min(600, 36 + 35 * min(len(df_lanc), 100)),
            column_config={
                "Valor (R$)": st.column_config.NumberColumn(format="R$ %.0f")
            }
        )
        st.caption(
            f"**{len(df_lanc)}** lançamentos · "
            f"Total: {fmt_brl(df_lanc['valor_abs'].sum() if 'valor_abs' in df_lanc.columns else 0)}"
        )

    # ════════════════════════════════════════════════════════
    # ABA 6 — Custo por m³
    # ════════════════════════════════════════════════════════
    with aba6:
        with st.spinner("Carregando volume de produção..."):
            vol_mes = carregar_producao_mensal()

        m3_c1, m3_c2 = st.columns(2)
        _grps_m3 = list(_COR_GRUPO.keys()) + ["Total (todos os grupos)"]
        _sel_ins = m3_c1.selectbox("Grupo para análise de custo/m³", _grps_m3,
                                   index=len(_grps_m3)-1, key="cd_sel_ins")

        # Seletor de Centro de Custo para filtrar o cálculo
        _palavras_prod = ["PRODUÇÃO","PRODUCAO","LABORATÓRIO","LABORATORIO",
                          "MONTAGEM","MANUTENÇÃO","MANUTENCAO"]
        _centros_prod = sorted([
            c for c in df_fil["centro_custos"].dropna().unique()
            if any(p in str(c).upper() for p in _palavras_prod)
        ])
        _sel_m3_centro = m3_c2.selectbox(
            "Centro de Custo (opcional)",
            ["Todos os centros"] + _centros_prod,
            key="cd_m3_centro"
        )
        df_m3 = df_fil.copy()
        if _sel_m3_centro != "Todos os centros":
            df_m3 = df_m3[df_m3["centro_custos"] == _sel_m3_centro]

        # Mês mais recente com volume
        _vol_pos = vol_mes[vol_mes > 0] if not vol_mes.empty else pd.Series(dtype=float)
        _mes_rec_v = _vol_pos.index.max() if not _vol_pos.empty else None

        # Bloco A — Métricas do mês mais recente
        if _mes_rec_v:
            _vol_rec = _vol_pos[_mes_rec_v]
            def _custo_mes_rec(grp_filtro=None, cg_filtro=None):
                _mask = df_m3["mes"] == _mes_rec_v
                if grp_filtro:
                    _mask &= df_m3["grupo_custo"] == grp_filtro
                if cg_filtro:
                    _mask &= df_m3["conta_gerencial"].str.upper().str.strip() == cg_filtro
                return df_m3.loc[_mask, "valor_abs"].sum()

            def _rpm3_hist(grp_filtro=None, cg_filtro=None):
                _sub = df_m3.copy()
                if grp_filtro:
                    _sub = _sub[_sub["grupo_custo"] == grp_filtro]
                if cg_filtro:
                    _sub = _sub[_sub["conta_gerencial"].str.upper().str.strip() == cg_filtro]
                _c_mes = _sub.groupby("mes")["valor_abs"].sum()
                _df_mv = pd.DataFrame({"custo": _c_mes}).join(vol_mes.rename("vol"), how="inner")
                _df_mv = _df_mv[_df_mv["vol"] > 0]
                if _df_mv.empty:
                    return None, None
                _r = _df_mv["custo"] / _df_mv["vol"]
                return _r.mean(), _r.std()

            st.subheader(f"Custo/m³ — {_mes_rec_v}")
            _m1, _m2, _m3c, _m4, _m5 = st.columns(5)
            for _col_met, _lbl, _grp_f, _cg_f in [
                (_m1,  "Pessoal + Ben./m³", None, None),
                (_m2,  "Insumos Estr./m³",  "Insumos Estruturais", None),
                (_m3c, "Materiais Cons./m³", "Materiais de Consumo", None),
                (_m4,  "Logística/m³",       "Logística", None),
                (_m5,  "Total Geral/m³",     None, None),
            ]:
                if _lbl == "Pessoal + Ben./m³":
                    _c = (df_m3.loc[(df_m3["mes"] == _mes_rec_v) &
                                    (df_m3["grupo_custo"].isin(["Pessoal","Benefícios"])),
                                    "valor_abs"].sum())
                    _med, _ = _rpm3_hist()
                elif _lbl == "Total Geral/m³":
                    _c = _custo_mes_rec()
                    _med, _ = _rpm3_hist()
                else:
                    _c = _custo_mes_rec(_grp_f, _cg_f)
                    _med, _ = _rpm3_hist(_grp_f, _cg_f)
                _rpm3_v = _c / _vol_rec if _vol_rec > 0 else 0
                _med_rpm3 = (_med / _vol_rec if _med and _vol_rec > 0 else None)
                _delta = (f"Δ vs média: {fmt_brl(_rpm3_v - _med_rpm3)}/m³"
                          if _med_rpm3 else None)
                _col_met.metric(_lbl, f"R$ {_rpm3_v:,.1f}", delta=_delta,
                                delta_color="inverse")

            st.divider()
            _m6, _m7, _m8, _m9, _m10 = st.columns(5)
            for _col_met, _lbl, _cg_name in [
                (_m6,  "Cimento/m³",     "CIMENTO"),
                (_m7,  "Aço/m³",         "AÇO PARA ESTRUTURAS"),
                (_m8,  "Cordoalha/m³",   "CORDOALHA"),
                (_m9,  "Areia/m³",       "AREIA NATURAL"),
                (_m10, "Serviços PJ/m³", None),
            ]:
                if _cg_name:
                    _c = _custo_mes_rec(cg_filtro=_cg_name)
                    _med, _ = _rpm3_hist(cg_filtro=_cg_name)
                else:
                    _c = _custo_mes_rec("Serviços PJ")
                    _med, _ = _rpm3_hist("Serviços PJ")
                _rpm3_v = _c / _vol_rec if _vol_rec > 0 else 0
                _med_rpm3 = (_med / _vol_rec if _med and _vol_rec > 0 else None)
                _delta = (f"Δ: {fmt_brl(_rpm3_v - _med_rpm3)}/m³" if _med_rpm3 else None)
                _col_met.metric(_lbl, f"R$ {_rpm3_v:,.1f}", delta=_delta,
                                delta_color="inverse")
        else:
            st.info("Nenhum mês com volume de produção encontrado no período.")

        # Bloco B — Gráfico de evolução custo/m³
        st.subheader("Evolução do Custo/m³")
        if _sel_ins == "Total (todos os grupos)":
            _c_mensal = df_m3.groupby("mes")["valor_abs"].sum()
        else:
            _c_mensal = (df_m3[df_m3["grupo_custo"] == _sel_ins]
                         .groupby("mes")["valor_abs"].sum())
        _df_rpm3 = pd.DataFrame({"custo": _c_mensal}).join(vol_mes.rename("vol"), how="inner")
        _df_rpm3 = _df_rpm3[_df_rpm3["vol"] > 0].copy()
        _df_rpm3["rpm3"] = _df_rpm3["custo"] / _df_rpm3["vol"]
        _df_rpm3 = _df_rpm3.sort_index()

        if not _df_rpm3.empty:
            _med_h  = _df_rpm3["rpm3"].mean()
            _std_h  = _df_rpm3["rpm3"].std()
            _meses_r = _df_rpm3.index.tolist()

            fig_rpm3 = go.Figure()
            # Banda ±1σ
            if not pd.isna(_std_h):
                fig_rpm3.add_trace(go.Scatter(
                    x=_meses_r + _meses_r[::-1],
                    y=([_med_h + _std_h] * len(_meses_r) +
                       [_med_h - _std_h] * len(_meses_r)),
                    fill="toself", fillcolor="rgba(33,150,243,0.1)",
                    line=dict(color="rgba(0,0,0,0)"),
                    name="±1σ", showlegend=True
                ))
            # Linha média
            fig_rpm3.add_hline(y=_med_h, line_dash="dash",
                                line_color="#616161", annotation_text="Média histórica")
            # Linha principal
            fig_rpm3.add_trace(go.Scatter(
                x=_meses_r, y=_df_rpm3["rpm3"],
                mode="lines+markers", name="R$/m³",
                line=dict(color="#1976D2", width=2),
                marker=dict(
                    color=["#E53935" if v > _med_h else "#43A047"
                           for v in _df_rpm3["rpm3"]],
                    size=8
                ),
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "R$/m³: R$ %{y:,.1f}<br>"
                    "Vol: %{customdata[0]:,.1f} m³<br>"
                    "Custo: R$ %{customdata[1]:,.0f}<extra></extra>"
                ),
                customdata=_df_rpm3[["vol","custo"]].values
            ))
            fig_rpm3.update_layout(height=360, hovermode="x unified",
                                   yaxis_title="R$/m³", xaxis_title="Mês",
                                   margin=dict(t=20, b=40))
            st.plotly_chart(fig_rpm3, use_container_width=True)

        # Bloco C — Tabela mensal
        st.subheader("Tabela Mensal Detalhada")
        if not _df_rpm3.empty:
            _df_rpm3_show = _df_rpm3.copy().reset_index()
            _df_rpm3_show.columns = ["Mês","Custo Total (R$)","Volume m³","R$/m³"]
            _df_rpm3_show["vs Média"] = _df_rpm3_show["R$/m³"] - _med_h
            _df_rpm3_show["Classe"]   = _df_rpm3_show["R$/m³"].apply(
                lambda v: "🟢 Bom" if v <= _med_h else
                          ("🟡 Atenção" if v <= _med_h * 1.2 else "🔴 Ruim"))
            st.dataframe(
                _df_rpm3_show[["Mês","Volume m³","Custo Total (R$)","R$/m³","vs Média","Classe"]]
                .sort_values("Mês", ascending=False),
                use_container_width=True, hide_index=True,
                column_config={
                    "Volume m³":       st.column_config.NumberColumn(format="%.1f"),
                    "Custo Total (R$)":st.column_config.NumberColumn(format="R$ %.0f"),
                    "R$/m³":           st.column_config.NumberColumn(format="R$ %.1f"),
                    "vs Média":        st.column_config.NumberColumn(format="R$ %.1f"),
                }
            )

        # Bloco D — Heatmap custo/m³
        st.subheader("Sazonalidade do Custo/m³")
        if not _df_rpm3.empty:
            _df_h2 = _df_rpm3.reset_index().copy()
            _df_h2["mes_dt"] = pd.to_datetime(_df_h2["mes"], format="%Y-%m", errors="coerce")
            _df_h2["mes_num"] = _df_h2["mes_dt"].dt.month
            _df_h2["ano_str"] = _df_h2["mes_dt"].dt.year.astype(str)
            _piv2 = _df_h2.pivot_table(index="ano_str", columns="mes_num",
                                        values="rpm3", aggfunc="mean")
            _piv2.columns = [_meses_pt.get(c, str(c)) for c in _piv2.columns]
            fig_h2 = px.imshow(_piv2, text_auto=".1f",
                               color_continuous_scale=["#43A047","#FFEE58","#E53935"],
                               aspect="auto")
            fig_h2.update_layout(height=max(180, min(70 * len(_piv2), 400)),
                                  margin=dict(t=20, b=20))
            st.plotly_chart(fig_h2, use_container_width=True)

    # ════════════════════════════════════════════════════════
    # ABA 7 — Tributos
    # ════════════════════════════════════════════════════════
    with aba7:
        st.info(
            "Os tributos sobre faturamento (ICMS, PIS, COFINS) não transitam como "
            "lançamentos individuais neste sistema — estão embutidos nas notas fiscais. "
            "Esta aba cobre os tributos que geram lançamentos explícitos: "
            "encargos sobre folha, retenções e guias."
        )

        def _classificar_tributo(row):
            cg  = str(row.get("conta_gerencial", "") or "").upper().strip()
            ctd = str(row.get("cod_tipo_doc",    "") or "").upper().strip()
            cm  = str(row.get("conta_macro",     "") or "").upper().strip()
            ps  = str(row.get("produto_servico", "") or "").upper().strip()
            if "INSS" in cg:
                return "Previdência (INSS)"
            if "FGTS" in cg:
                return "FGTS"
            if "IRRF" in cg:
                return "IR/IRRF"
            if any(k in cg for k in ["SESI","SENAI","CONTRIBUIÇÕES A ENTIDADES"]):
                return "Contribuições Sociais"
            if any(k in cg for k in ["TAXAS","ALVARÁ","LICENÇA"]):
                return "Taxas e Licenças"
            if "DARF" in ctd:
                return "IR/IRRF"
            if any(k in ctd for k in ["GPS","PREVIDENCIA"]):
                return "Previdência Social (GPS)"
            if "DAE" in ctd:
                return "Taxas e Licenças"
            if "GUIA DE RECOLHIMENTO" in ctd:
                return "Previdência Social (GPS)"
            if "FGTS" in ctd:
                return "FGTS"
            if "DESPESAS TRIBUTÁRIAS" in cm or "DESPESAS TRIBUTARIAS" in cm:
                return "Outros Tributos"
            if "CIEE" in ps:
                return "Contribuições Sociais"
            return None

        def extrair_tributos(df_custos, df_despesas):
            _cols_t = ["data","mes","ano","centro_custos","conta_gerencial",
                       "cod_tipo_doc","produto_servico","cli_fornecedor",
                       "valor_global","conta_macro"]
            parts = []
            for _df, _orig in [(df_custos, "Custo"), (df_despesas, "Despesa")]:
                if _df is None or _df.empty:
                    continue
                _sub = _df[[c for c in _cols_t if c in _df.columns]].copy()
                _sub["origem"] = _orig
                _sub["grupo_tributo"] = _sub.apply(_classificar_tributo, axis=1)
                parts.append(_sub[_sub["grupo_tributo"].notna()])
            if not parts:
                return pd.DataFrame()
            return pd.concat(parts, ignore_index=True)

        _df_trib = extrair_tributos(
            df_cus if not df_cus.empty else None,
            df_des if not df_des.empty else None
        )

        if _dt_ini and not _df_trib.empty:
            _df_trib = _df_trib[_df_trib["data"] >= pd.Timestamp(_dt_ini)]
        if _dt_fim and not _df_trib.empty:
            _df_trib = _df_trib[_df_trib["data"] <= pd.Timestamp(_dt_fim)]

        if _df_trib.empty:
            st.warning("Nenhum lançamento tributário identificado no período.")
        else:
            _tot_trib = _df_trib["valor_global"].sum()

            # KPIs
            _rec_periodo = pd.Series(dtype=float)
            if not df_rec.empty:
                _rec_f = df_rec.copy()
                if _dt_ini:
                    _rec_f = _rec_f[_rec_f["data"] >= pd.Timestamp(_dt_ini)]
                if _dt_fim:
                    _rec_f = _rec_f[_rec_f["data"] <= pd.Timestamp(_dt_fim)]
                _rec_periodo = _rec_f[_rec_f["valor_global"] > 0]["valor_global"].sum()
            else:
                _rec_periodo = 0

            _pct_receita = (_tot_trib / _rec_periodo * 100) if _rec_periodo > 0 else 0
            _pct_custo_t = (_tot_trib / _tot * 100) if _tot > 0 else 0
            _grupo_maior = _df_trib.groupby("grupo_tributo")["valor_global"].sum().idxmax()
            _grupo_maior_val = _df_trib.groupby("grupo_tributo")["valor_global"].sum().max()
            _meses_trib_ord = sorted(_df_trib["mes"].dropna().unique(), reverse=True)
            if len(_meses_trib_ord) >= 2:
                _val_rec_t = _df_trib[_df_trib["mes"] == _meses_trib_ord[0]]["valor_global"].sum()
                _val_ant_t = _df_trib[_df_trib["mes"] == _meses_trib_ord[1]]["valor_global"].sum()
                _var_trib  = ((_val_rec_t - _val_ant_t) / _val_ant_t * 100) if _val_ant_t > 0 else 0
                _delta_trib = f"{_var_trib:+.1f}% vs mês anterior"
            else:
                _delta_trib = None

            kt1, kt2, kt3, kt4, kt5 = st.columns(5)
            kt1.metric("Total de Tributos", fmt_brl(_tot_trib))
            kt2.metric("% sobre Receita Bruta", f"{_pct_receita:.1f}%")
            kt3.metric("% sobre Custo Total",   f"{_pct_custo_t:.1f}%")
            kt4.metric("Maior Grupo", f"{_grupo_maior}\n{fmt_brl(_grupo_maior_val)}")
            kt5.metric("Variação vs Mês Ant.", _delta_trib or "—")

            # Bloco A — Composição por grupo
            st.subheader("Composição por Grupo")
            _trib_grp = _df_trib.groupby("grupo_tributo")["valor_global"].sum()
            tc1, tc2 = st.columns(2)
            with tc1:
                fig_tp = go.Figure(go.Pie(
                    labels=_trib_grp.index, values=_trib_grp.values,
                    hole=0.42,
                    marker_colors=[_COR_TRIB.get(g, "#9E9E9E") for g in _trib_grp.index],
                    textinfo="label+percent"
                ))
                fig_tp.update_layout(title="Proporção por grupo", height=350,
                                     margin=dict(t=40, b=10))
                st.plotly_chart(fig_tp, use_container_width=True)
            with tc2:
                _trib_ord = _trib_grp.sort_values(ascending=True)
                fig_tb = go.Figure(go.Bar(
                    x=_trib_ord.values, y=_trib_ord.index,
                    orientation="h",
                    marker_color=[_COR_TRIB.get(g, "#9E9E9E") for g in _trib_ord.index],
                    hovertemplate="<b>%{y}</b><br>R$ %{x:,.0f}<extra></extra>"
                ))
                fig_tb.update_layout(height=350, xaxis_title="Valor (R$)",
                                     margin=dict(t=40, b=10))
                st.plotly_chart(fig_tb, use_container_width=True)

            # Bloco B — Evolução mensal
            st.subheader("Evolução Mensal por Grupo")
            _trib_ev = (_df_trib.groupby(["mes","grupo_tributo"])["valor_global"]
                        .sum().reset_index())
            _trib_tot = _df_trib.groupby("mes")["valor_global"].sum()
            _meses_t_ord = sorted(_trib_ev["mes"].unique())
            fig_tev = go.Figure()
            for _grp_t, _cor_t in _COR_TRIB.items():
                _d = _trib_ev[_trib_ev["grupo_tributo"] == _grp_t]
                if _d.empty:
                    continue
                _d = (_d.set_index("mes")[["valor_global"]]
                      .reindex(_meses_t_ord, fill_value=0).reset_index())
                fig_tev.add_trace(go.Bar(
                    x=_d["mes"], y=_d["valor_global"],
                    name=_grp_t, marker_color=_cor_t,
                    hovertemplate=f"<b>{_grp_t}</b><br>%{{x}}<br>R$ %{{y:,.0f}}<extra></extra>"
                ))
            _trib_tot_ord = _trib_tot.reindex(_meses_t_ord, fill_value=0)
            fig_tev.add_trace(go.Scatter(
                x=_meses_t_ord, y=_trib_tot_ord.values,
                name="Total", mode="lines+markers",
                line=dict(color="#212121", width=2, dash="dot"),
                yaxis="y2",
                hovertemplate="<b>Total</b><br>%{x}<br>R$ %{y:,.0f}<extra></extra>"
            ))
            fig_tev.update_layout(
                barmode="stack", hovermode="x unified", height=340,
                yaxis=dict(title="R$"),
                yaxis2=dict(title="Total", overlaying="y", side="right", showgrid=False),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(t=50, b=40)
            )
            st.plotly_chart(fig_tev, use_container_width=True)

            # Bloco C — Carga tributária efetiva
            st.subheader("Carga Tributária Efetiva (% sobre Receita Bruta)")
            if not df_rec.empty:
                _rec_mes = _rec_f[_rec_f["valor_global"] > 0].groupby("mes")["valor_global"].sum()
                _trib_mes = _df_trib.groupby("mes")["valor_global"].sum()
                _carga_ef = (_trib_mes / _rec_mes * 100).dropna()
                _carga_ef = _carga_ef[_carga_ef.index.isin(_trib_mes.index)]

                if not _carga_ef.empty:
                    _med_carga = _carga_ef.mean()
                    _lim_carga = 15.0
                    _meses_ce  = sorted(_carga_ef.index.tolist())
                    fig_cef = go.Figure()
                    fig_cef.add_hline(y=_med_carga, line_dash="dash",
                                      line_color="#616161", annotation_text="Média")
                    fig_cef.add_hline(y=_lim_carga, line_dash="dot",
                                      line_color="#E53935", annotation_text="15% ref.")
                    fig_cef.add_trace(go.Scatter(
                        x=_meses_ce,
                        y=[_carga_ef.get(m, None) for m in _meses_ce],
                        mode="lines+markers", name="Carga tributária (%)",
                        line=dict(color="#1976D2", width=2),
                        marker=dict(
                            color=["#E53935" if (_carga_ef.get(m,0) or 0) > _lim_carga
                                   else "#43A047" for m in _meses_ce],
                            size=8
                        ),
                        hovertemplate="<b>%{x}</b><br>%{y:.1f}%<extra></extra>"
                    ))
                    fig_cef.update_layout(height=280, hovermode="x unified",
                                          yaxis_title="% da Receita",
                                          margin=dict(t=20, b=40))
                    st.plotly_chart(fig_cef, use_container_width=True)
                    st.caption(
                        "Carga tributária = tributos pagos ÷ receita bruta do período. "
                        "Não inclui tributos embutidos no preço (ICMS, PIS, COFINS sobre vendas isentas)."
                    )
                else:
                    st.info("Sem dados suficientes para calcular carga tributária.")
            else:
                st.info("Tabela de receitas não encontrada — carga tributária indisponível.")

            # Bloco D — Tabela detalhada
            st.subheader("Detalhamento dos Lançamentos")
            td1, td2, td3 = st.columns(3)
            _grps_trib_opts = ["Todos"] + sorted(_df_trib["grupo_tributo"].dropna().unique().tolist())
            _fil_gt = td1.selectbox("Grupo", _grps_trib_opts, key="cd_trib_grp")
            _fil_tm = td2.selectbox("Mês", ["Todos"] + sorted(
                _df_trib["mes"].dropna().unique().tolist(), reverse=True), key="cd_trib_mes")
            _fil_to = td3.selectbox("Origem", ["Todos","Custo","Despesa"], key="cd_trib_orig")

            _df_trib_f = _df_trib.copy()
            if _fil_gt != "Todos":
                _df_trib_f = _df_trib_f[_df_trib_f["grupo_tributo"] == _fil_gt]
            if _fil_tm != "Todos":
                _df_trib_f = _df_trib_f[_df_trib_f["mes"] == _fil_tm]
            if _fil_to != "Todos":
                _df_trib_f = _df_trib_f[_df_trib_f["origem"] == _fil_to]

            _cols_trib_show = ["data","origem","grupo_tributo","conta_gerencial",
                               "cli_fornecedor","centro_custos","cod_tipo_doc","valor_global"]
            _cols_trib_ok = [c for c in _cols_trib_show if c in _df_trib_f.columns]
            st.dataframe(
                _df_trib_f[_cols_trib_ok].rename(columns={
                    "data":           "Data",
                    "origem":         "Origem",
                    "grupo_tributo":  "Grupo",
                    "conta_gerencial":"Conta Gerencial",
                    "cli_fornecedor": "Fornecedor/Beneficiário",
                    "centro_custos":  "Centro de Custo",
                    "cod_tipo_doc":   "Documento",
                    "valor_global":   "Valor (R$)",
                }).sort_values("Data", ascending=False),
                use_container_width=True, hide_index=True,
                column_config={"Valor (R$)": st.column_config.NumberColumn(format="R$ %.0f")}
            )
            st.caption(
                f"**{len(_df_trib_f)}** lançamentos · "
                f"Total filtrado: {fmt_brl(_df_trib_f['valor_global'].sum())}"
            )
