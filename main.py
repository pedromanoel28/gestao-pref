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
    "--- 📊 DASHBOARDS ---", "🏭 Produção", "💰 Financeiro", "🔍 Análise da Obra", "👷 Folha / RH",
    "--- 🛠️ GESTÃO ---", "🏗️ Gestão de Obras", "🛤️ Jornada da Obra", "👥 Equipe", 
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

def mapa_obras():
    """Dicionário código-4-dígitos → id do banco."""
    dados = supabase.table("obras").select("id, cod4").execute().data
    m = {}
    for o in dados:
        # Puxa a nova coluna cod4
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

def enviar_lotes(tabela, pacote, barra_label="Enviando...", on_conflict=None):
    """Envia em lotes de 500 com barra de progresso.
    Se on_conflict for fornecido, usa upsert; caso contrário insert."""
    total = len(pacote)
    barra = st.progress(0, text=barra_label)
    enviado = 0
    for i in range(0, total, 500):
        lote = pacote[i:i+500]
        if on_conflict:
            supabase.table(tabela).upsert(lote, on_conflict=on_conflict).execute()
        else:
            supabase.table(tabela).insert(lote).execute()
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
        if val is None or str(val).strip().lower() in ("", "nan", "nat", "none", "inf"):
            return None
        try:
            v = float(str(val).replace("R$","").replace(" ","")
                      .replace(".","").replace(",",".").strip())
            return None if (_math.isnan(v) or _math.isinf(v)) else v
        except Exception:
            return None

    def _limpar_str_imp(val):
        if val is None or str(val).strip().lower() in ("", "nan", "nat", "none"):
            return None
        return str(val).strip()

    def _inline_cache_clear():
        try: carregar_obras_ativas.clear()
        except: pass
        try: carregar_obras_completo.clear()
        except: pass
        try: carregar_medicoes_resumo.clear()
        except: pass
        try: carregar_producao_resumo.clear()
        except: pass
        try: carregar_transporte_resumo.clear()
        except: pass
        try: carregar_montagem_resumo.clear()
        except: pass
        try: carregar_custos_resumo.clear()
        except: pass
        try: carregar_equipe_ativa.clear()
        except: pass

    # ── seleção de rota ───────────────────────────────────────────────────────
    opcao = st.selectbox("Qual arquivo está importando?", [
        "1. Equipe              → qualquer CSV com colunas nome / email / setor / cargo",
        "2. Financeiro Obras    → Civil_Comercial_*.csv",
        "3. Medições            → Lista_Medição.csv",
        "4. Fabricação          → exportação ERP peça a peça",
        "5. Transporte          → exportação ERP expedição",
        "6. Montagem            → exportação ERP montagem",
        "7. Custos              → exportação ERP lançamentos",
    ])
    rota = opcao.strip()[0]          # "1" … "7"

    arquivo = st.file_uploader("Arraste o CSV aqui", type=["csv"])

    if arquivo:
        # ── leitura automática (sep ; ou ,) ───────────────────────────────────
        try:
            arquivo.seek(0)
            df_prev = pd.read_csv(arquivo, sep=None, engine="python",
                                  encoding="utf-8-sig", dtype=str, header=0)
            with st.expander(f"👁️ Pré-visualização — {df_prev.shape[0]} linhas × {df_prev.shape[1]} colunas"):
                st.dataframe(df_prev.head(10), use_container_width=True)
        except Exception as e:
            st.error(f"❌ Erro ao ler CSV: {e}")
            st.stop()

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
                                    payload2[campo] = _limpar_num_imp(row2.get(campo))
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
                        mask_cancel3 = (df3["tipo"].str.strip().str.upper()
                                        == "NOTA FISCAL CANCELADA")
                        n_cancel3 = int(mask_cancel3.sum())
                        df3 = df3[~mask_cancel3].copy()
                        df3["data_emissao"]    = formatar_data(df3["data_emissao"])
                        df3["data_vencimento"] = formatar_data(df3["data_vencimento"])
                        df3["valor"]           = formatar_numero(df3["valor"])
                        mob3 = mapa_obras()
                        df3["obra_id"] = aplicar_obra_id(
                            df3["titulo"].apply(extrair_codigo), mob3)
                        sem_obra3 = int(df3["obra_id"].isna().sum())
                        cols_bd3 = [
                            "obra_id","codigo_obra_original","titulo","etapa_obra",
                            "data_emissao","nome_pagador","cnpj_pagador","numero_nf",
                            "numero_nf_remessa","data_vencimento","descricao","valor",
                            "cnpj_recebedor","razao_social_recebedor","tipo",
                            "observacoes","categoria",
                        ]
                        cols_bd3 = [c for c in cols_bd3 if c in df3.columns]
                        pacote3 = df3[cols_bd3].to_dict("records")
                        pacote3 = fix_ids(pacote3)
                        pacote3 = limpar_nan_pacote(pacote3)
                        total3 = enviar_lotes("medicoes", pacote3, "Enviando medições...")
                        _inline_cache_clear()
                        st.success(f"🎉 {total3} medições importadas!")
                        st.info(
                            f"✅ {total3 - sem_obra3} com obra  "
                            f"| ⚠️ {sem_obra3} sem obra  "
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
                        mob_p = mapa_obras()
                        df_p["obra_id"]   = aplicar_obra_id(
                            df_p["obra_codigo"].apply(extrair_codigo), mob_p)
                        df_p["cod4_obra"] = df_p["obra_codigo"].apply(extrair_codigo)
                        df_p["cod4_obra"] = df_p["cod4_obra"].apply(
                            lambda v: None if (v is None or str(v).strip().lower()
                                               in ("nan","none","")) else str(v)
                        )
                        sem_obra_p = int(df_p["obra_id"].isna().sum())
                        df_p[date_p] = formatar_data(df_p[date_p])
                        for c_p in num_p:
                            if c_p in df_p.columns:
                                df_p[c_p] = formatar_numero(df_p[c_p])
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
                        df7 = pd.read_csv(arquivo, sep=None, engine="python",
                                          encoding="utf-8-sig", dtype=str, header=0)
                        # Mapeamento flexível de colunas (vários formatos de ERP)
                        rename7 = {}
                        for c7 in df7.columns:
                            cl7 = c7.lower().strip()
                            if cl7 in ("obra","obra_codigo","cod_obra","codigo_obra"):
                                rename7[c7] = "obra_codigo"
                            elif cl7 in ("data","data_lancamento","dt_lancamento","data_doc"):
                                rename7[c7] = "data"
                            elif cl7 in ("id_lancamento","id","lancamento","id_lanc"):
                                rename7[c7] = "id_lancamento"
                            elif cl7 in ("numero_doc","num_doc","numero","ndoc","n_doc"):
                                rename7[c7] = "numero_doc"
                            elif cl7 in ("centro_custos","centro","cc","centro_de_custo"):
                                rename7[c7] = "centro_custos"
                            elif cl7 in ("conta_macro","macro","conta_gerencial_macro"):
                                rename7[c7] = "conta_macro"
                            elif cl7 in ("conta_gerencial","conta","gerencial"):
                                rename7[c7] = "conta_gerencial"
                            elif cl7 in ("cli_fornecedor","fornecedor","cliente","cli_forn",
                                         "nome_fornecedor","nome_cliente"):
                                rename7[c7] = "cli_fornecedor"
                            elif cl7 in ("produto_servico","produto","servico","descricao",
                                         "historico","desc"):
                                rename7[c7] = "produto_servico"
                            elif cl7 in ("criado_por","usuario","user","operador"):
                                rename7[c7] = "criado_por"
                            elif cl7 in ("valor_global","valor","total","vl_total","vl_global"):
                                rename7[c7] = "valor_global"
                            elif cl7 in ("qtd","quantidade","qtde","qty"):
                                rename7[c7] = "qtd"
                            elif cl7 in ("preco_unitario","preco","unit","vl_unit","valor_unit"):
                                rename7[c7] = "preco_unitario"
                            elif cl7 in ("origem","tipo_origem","fonte"):
                                rename7[c7] = "origem"
                            elif cl7 in ("chave_coligada","coligada","chave"):
                                rename7[c7] = "chave_coligada"
                            elif cl7 in ("cod_tipo_doc","tipo_doc","cod_tipo","tipo_documento"):
                                rename7[c7] = "cod_tipo_doc"
                        df7 = df7.rename(columns=rename7)
                        if "obra_codigo" not in df7.columns:
                            st.error("❌ Coluna de código de obra não encontrada. "
                                     "Renomeie para 'obra_codigo' e tente novamente.")
                            st.stop()
                        mob7 = mapa_obras()
                        df7["obra_id"] = aplicar_obra_id(
                            df7["obra_codigo"].apply(extrair_codigo), mob7)
                        df7 = df7.replace("", None).where(pd.notnull(df7), None)
                        com_obra7 = int(df7["obra_id"].notna().sum())
                        sem_obra7 = int(df7["obra_id"].isna().sum())
                        for col7 in ["valor_global","qtd","preco_unitario"]:
                            if col7 in df7.columns:
                                df7[col7] = pd.to_numeric(df7[col7], errors="coerce")
                                df7[col7] = df7[col7].where(pd.notnull(df7[col7]), None)
                        cols_bd7 = ["obra_id","data","id_lancamento","numero_doc",
                                    "centro_custos","conta_macro","conta_gerencial",
                                    "cli_fornecedor","produto_servico","criado_por",
                                    "valor_global","qtd","preco_unitario","origem",
                                    "chave_coligada","cod_tipo_doc"]
                        cols_bd7_ok = [c for c in cols_bd7 if c in df7.columns]
                        pacote7_raw = df7[cols_bd7_ok].to_dict("records")
                        pacote7 = []
                        for row7 in pacote7_raw:
                            pacote7.append({
                                k: None if (
                                    v is None
                                    or (isinstance(v, float)
                                        and (_math.isnan(v) or _math.isinf(v)))
                                    or str(v).strip().lower()
                                    in ("nan","nat","none","inf","")
                                ) else v
                                for k, v in row7.items()
                            })
                        total7 = enviar_lotes("custos", pacote7, "Enviando custos...")
                        _inline_cache_clear()
                        st.success(f"🎉 {total7} lançamentos importados!")
                        st.info(f"✅ {com_obra7} diretos (com obra) | 🏭 {sem_obra7} indiretos")

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

@st.cache_data(ttl=60)
def carregar_obras_ativas():
    resp = supabase.table("obras")\
        .select("id, cod4, nome, status, modalidade, cliente, responsavel_id")\
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
                "obras(nome, cod4)")\
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
    try: carregar_obras_completo.clear()
    except: pass
    try: carregar_medicoes_resumo.clear()
    except: pass
    try: carregar_producao_resumo.clear()
    except: pass
    try: carregar_transporte_resumo.clear()
    except: pass
    try: carregar_montagem_resumo.clear()
    except: pass
    try: carregar_custos_resumo.clear()
    except: pass
    try: carregar_tarefas_extras.clear()
    except: pass
    try: _jornada_analise.clear()
    except: pass
    try: _marcos_analise.clear()
    except: pass
    try: _fab_obra.clear()
    except: pass
    try: _exp_obra.clear()
    except: pass
    try: _mont_obra.clear()
    except: pass
    try: _medicoes_obra.clear()
    except: pass
    try: _fin_obra.clear()
    except: pass

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

@st.cache_data(ttl=300)
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

@st.cache_data(ttl=300)
def carregar_medicoes_resumo():
    """medicoes sem canceladas/remessas, com campo mes."""
    rows, page, size = [], 0, 1000
    q = supabase.table("medicoes")\
        .select("obra_id, data_emissao, descricao, tipo, valor")
    while True:
        resp = q.range(page * size, (page + 1) * size - 1).execute()
        rows.extend(resp.data)
        if len(resp.data) < size: break
        page += 1
    df = pd.DataFrame(rows)
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
    """producao_fabricacao com dedup por (obra_id, peca) — volume_teorico correto."""
    size = 1000
    try:
        cols = ("obra_id, peca, produto, secao, etapa, qtde_pecas,"
                " volume_teorico, volume_total, peso_aco_frouxo,"
                " peso_aco_protendido, peso_aco, data_fabricacao")
        # testa se colunas existem
        supabase.table("producao_fabricacao").select(cols).range(0, 0).execute()
    except Exception:
        cols = ("obra_id, peca, produto, etapa, volume_teorico,"
                " volume_total, peso_aco, data_fabricacao")
    rows, page = [], 0
    q = supabase.table("producao_fabricacao").select(cols)
    while True:
        resp = q.range(page * size, (page + 1) * size - 1).execute()
        rows.extend(resp.data)
        if len(resp.data) < size: break
        page += 1
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ["volume_teorico","volume_total","peso_aco",
                "peso_aco_frouxo","peso_aco_protendido","qtde_pecas"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["data_fabricacao"] = pd.to_datetime(df["data_fabricacao"], errors="coerce")
    df["mes"] = df["data_fabricacao"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=300)
def carregar_transporte_resumo():
    """producao_transporte resumido para dashboards."""
    rows, page, size = [], 0, 1000
    q = supabase.table("producao_transporte")\
        .select("obra_id, produto, volume_real, data_expedicao")
    while True:
        resp = q.range(page * size, (page + 1) * size - 1).execute()
        rows.extend(resp.data)
        if len(resp.data) < size: break
        page += 1
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["volume_real"]    = pd.to_numeric(df["volume_real"], errors="coerce").fillna(0)
    df["data_expedicao"] = pd.to_datetime(df["data_expedicao"], errors="coerce")
    df["mes"]            = df["data_expedicao"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=300)
def carregar_montagem_resumo():
    """producao_montagem resumido para dashboards."""
    rows, page, size = [], 0, 1000
    q = supabase.table("producao_montagem")\
        .select("obra_id, produto, volume_teorico, data_montagem")
    while True:
        resp = q.range(page * size, (page + 1) * size - 1).execute()
        rows.extend(resp.data)
        if len(resp.data) < size: break
        page += 1
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["volume_teorico"] = pd.to_numeric(df["volume_teorico"], errors="coerce").fillna(0)
    df["data_montagem"]  = pd.to_datetime(df["data_montagem"], errors="coerce")
    df["mes"]            = df["data_montagem"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=300)
def carregar_custos_resumo():
    """custos para eficiência — valor_global em abs()."""
    rows, page, size = [], 0, 1000
    q = supabase.table("custos")\
        .select("data, conta_macro, conta_gerencial, centro_custos, cli_fornecedor, valor_global")
    while True:
        resp = q.range(page * size, (page + 1) * size - 1).execute()
        rows.extend(resp.data)
        if len(resp.data) < size: break
        page += 1
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["data"]         = pd.to_datetime(df["data"], errors="coerce")
    df["valor_global"] = pd.to_numeric(df["valor_global"], errors="coerce").abs().fillna(0)
    df["mes"]          = df["data"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=60)
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
        import plotly.graph_objects as _go
        import plotly.express as _px

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
    import plotly.graph_objects as go
    import plotly.express as px

    # ── FUNÇÕES DE CARGA ────────────────────────────────────

    @st.cache_data(ttl=300)
    def carregar_obras_financeiro():
        resp_of = supabase.table("obras_financeiro").select("*").execute()
        if not resp_of.data:
            return pd.DataFrame()
        resp_o = supabase.table("obras")\
            .select("id, cod4, nome, status, modalidade, cliente").execute()
        df_of = pd.DataFrame(resp_of.data)
        df_o  = pd.DataFrame(resp_o.data or [])
        if not df_o.empty and "cod4" in df_of.columns:
            df = df_of.merge(df_o, on="cod4", how="left", suffixes=("", "_obras"))
            df = df.rename(columns={
                "id":        "obra_id",
                "nome":      "obra_nome",
                "status":    "obra_status",
            })
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

    @st.cache_data(ttl=300)
    def carregar_medicoes():
        # Filtragem de tipo feita em Python — not_.in_ combinado com range()
        # causa APIError em certas versões do supabase-py
        rows, page, size = [], 0, 1000
        q = supabase.table("medicoes").select("obra_id, data_emissao, descricao, tipo, valor")
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty: return df
        excluir = {"NOTA FISCAL CANCELADA", "NOTA FISCAL REMESSA"}
        df = df[~df["tipo"].str.strip().str.upper().isin(excluir)].copy()
        df["valor"]        = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
        df["mes"]          = df["data_emissao"].dt.to_period("M").astype(str)
        return df

    @st.cache_data(ttl=300)
    def carregar_medicoes_completas(obra_id):
        # Seleciona colunas explícitas (sem *) e filtra tipo em Python
        cols = ("obra_id, data_emissao, numero_nf, numero_nf_remessa, "
                "descricao, tipo, valor, nome_pagador, cnpj_recebedor, "
                "razao_social_recebedor, etapa_obra, categoria, observacoes")
        rows, page, size = [], 0, 1000
        q = supabase.table("medicoes").select(cols).eq("obra_id", obra_id)
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty: return df
        df = df[df["tipo"].str.strip().str.upper() != "NOTA FISCAL CANCELADA"].copy()
        df["valor"]        = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
        df = df.sort_values("data_emissao", ascending=False).reset_index(drop=True)
        return df

    def _carregar_volumes(tabela, col_vol, col_data):
        """Carrega (obra_id, peca, col_vol) com deduplicação por (obra_id, peca).
        Deduplicar evita contagem dobrada quando a importação foi executada mais de uma vez."""
        rows, page, size = [], 0, 1000
        q = supabase.table(tabela).select(f"obra_id, peca, {col_vol}")
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        if not rows: return {}
        df = pd.DataFrame(rows)
        df[col_vol] = pd.to_numeric(df[col_vol], errors="coerce").fillna(0)
        return df.groupby("obra_id")[col_vol].sum().to_dict()

    @st.cache_data(ttl=300)
    def volume_fabricado_por_obra():
        return _carregar_volumes("producao_fabricacao", "volume_teorico", "data_fabricacao")

    @st.cache_data(ttl=300)
    def volume_expedido_por_obra():
        return _carregar_volumes("producao_transporte", "volume_real", "data_expedicao")

    @st.cache_data(ttl=300)
    def volume_montado_por_obra():
        return _carregar_volumes("producao_montagem", "volume_teorico", "data_montagem")

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
        volume_expedido_por_obra.clear()
        volume_montado_por_obra.clear()
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
    obras_map   = {o["id"]: f"{o['cod4']} — {o['nome']}" for o in obras_lista}

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

        fab_obra = (df_fab_f.groupby("obra_id")["volume_teorico"].sum()
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
        # ==========================================================
# PÁGINA: JORNADA DA OBRA (VISÃO MACRO E GANTT)
# ==========================================================
elif pagina_selecionada == "🛤️ Jornada da Obra":
    import pandas as pd
    from datetime import date
    import plotly.express as px
    import plotly.graph_objects as go

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

    c1, c2, c3 = st.columns([3, 1, 1])
    obras_map = {f"{o['cod4']} — {o['nome']}": o for o in obras}
    obra_sel_str = c1.selectbox("Selecione a Obra", list(obras_map.keys()), label_visibility="collapsed")
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
    import pandas as pd
    import plotly.express as px
    from datetime import date

    st.header("🔍 Análise da Obra")

    # ── CACHE FUNCTIONS ──────────────────────────────────────
    @st.cache_data(ttl=60)
    def _jornada_analise(obra_id):
        resp = supabase.table("obra_jornada")\
            .select("*, template:jornada_template(*)")\
            .eq("obra_id", obra_id).execute()
        return sorted(resp.data, key=lambda x: x["template"]["ordem"]) if resp.data else []

    @st.cache_data(ttl=60)
    def _marcos_analise(obra_id):
        resp = supabase.table("obra_marcos").select("*").eq("obra_id", obra_id).execute()
        return resp.data[0] if resp.data else {}

    @st.cache_data(ttl=300)
    def _fab_obra(obra_id):
        rows, page, size = [], 0, 1000
        q = supabase.table("producao_fabricacao")\
            .select("peca, produto, volume_teorico, data_fabricacao")\
            .eq("obra_id", obra_id)
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if not df.empty:
            df["volume_teorico"] = pd.to_numeric(df["volume_teorico"], errors="coerce").fillna(0)
            df["data_fabricacao"] = pd.to_datetime(df["data_fabricacao"], errors="coerce")
        return df

    @st.cache_data(ttl=300)
    def _exp_obra(obra_id):
        rows, page, size = [], 0, 1000
        q = supabase.table("producao_transporte")\
            .select("produto, volume_real, data_expedicao")\
            .eq("obra_id", obra_id)
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if not df.empty:
            df["volume_real"] = pd.to_numeric(df["volume_real"], errors="coerce").fillna(0)
            df["data_expedicao"] = pd.to_datetime(df["data_expedicao"], errors="coerce")
        return df

    @st.cache_data(ttl=300)
    def _mont_obra(obra_id):
        rows, page, size = [], 0, 1000
        q = supabase.table("producao_montagem")\
            .select("produto, volume_teorico, data_montagem")\
            .eq("obra_id", obra_id)
        while True:
            resp = q.range(page * size, (page + 1) * size - 1).execute()
            rows.extend(resp.data)
            if len(resp.data) < size: break
            page += 1
        df = pd.DataFrame(rows)
        if not df.empty:
            df["volume_teorico"] = pd.to_numeric(df["volume_teorico"], errors="coerce").fillna(0)
            df["data_montagem"] = pd.to_datetime(df["data_montagem"], errors="coerce")
        return df

    @st.cache_data(ttl=300)
    def _medicoes_obra(obra_id):
        try:
            rows, page, size = [], 0, 1000
            q = supabase.table("medicoes")\
                .select("numero_nf, tipo, valor, data_emissao")\
                .eq("obra_id", obra_id)
            while True:
                resp = q.range(page * size, (page + 1) * size - 1).execute()
                rows.extend(resp.data)
                if len(resp.data) < size: break
                page += 1
            df = pd.DataFrame(rows)
            if df.empty: return df
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
            df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")
            return df
        except Exception:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
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

    c_sel, c_btn = st.columns([5, 1])
    obras_map_an = {f"{o['cod4']} — {o['nome']}": o for o in obras_an}
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