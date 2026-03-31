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

                    except Exception as e:
                        st.error(f"❌ Erro no banco: {e}")
        except Exception as e:
            st.error(f"❌ Erro na leitura do CSV: {e}")

# ==========================================================
# EQUIPE
# ==========================================================
elif pagina_selecionada == "👥 Equipe":
    st.header("👥 Gestão da Equipe")
    resp = supabase.table("equipe").select("*").order("nome").execute()
    df_eq = pd.DataFrame(resp.data)
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
                st.rerun()

# ==========================================================
# FUNÇÕES AUXILIARES DA SÚMULA
# ==========================================================

@st.cache_data(ttl=60)
def carregar_obras_ativas():
    resp = supabase.table("obras").select("id, codigo, nome, status").order("nome").execute()
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
        .select("*, responsavel:equipe!obras_tarefas_responsavel_id_fkey(nome), aprovador:equipe!obras_tarefas_aprovador_id_fkey(nome)")\
        .eq("obra_id", obra_id)\
        .order("gut_score", desc=True)\
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
    except:
        return []

def calcular_farol(entrega_prevista, status):
    if status in ("Concluído","N/A"): return "✅"
    if not entrega_prevista: return "⚪"
    from datetime import date
    try: diff = (date.fromisoformat(str(entrega_prevista)) - date.today()).days
    except: return "⚪"
    return "🔴" if diff < 0 else ("🟡" if diff <= 2 else "🟢")

def calcular_desvio(entrega_prevista, entrega_real, status):
    if not entrega_prevista: return "—"
    from datetime import date
    try: prev = date.fromisoformat(str(entrega_prevista))
    except: return "—"
    if entrega_real:
        try: diff = (date.fromisoformat(str(entrega_real)) - prev).days
        except: return "—"
    else:
        if status in ("Concluído","N/A"): return "—"
        diff = (date.today() - prev).days
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

def limpar_cache():
    carregar_obras_ativas.clear()
    carregar_equipe_ativa.clear()
    carregar_template.clear()
    carregar_tarefas.clear()
    carregar_alertas.clear()

# ==========================================================
# PÁGINA: GESTÃO DE OBRAS (SÚMULA)
# ==========================================================
if pagina_selecionada == "🏗️ Gestão de Obras":
    from datetime import date

    STATUS_OPCOES = ["A Iniciar","Em Andamento","Impedido","Concluído","N/A"]

    obras        = carregar_obras_ativas()
    equipe_lista = carregar_equipe_ativa()
    equipe_nomes = [e["nome"] for e in equipe_lista]
    equipe_ids   = {e["nome"]: e["id"] for e in equipe_lista}

    if not obras:
        st.error("Nenhuma obra cadastrada."); st.stop()

    # ── SIDEBAR: SELEÇÃO + CABEÇALHO FIXO ────────────────
    st.sidebar.divider()
    st.sidebar.markdown("### 🏗️ Súmula")

    # Botão atualizar
    if st.sidebar.button("🔄 Atualizar dados"):
        limpar_cache(); st.rerun()

    # Filtro status obra
    status_disponiveis = sorted(set(o.get("status") or "—" for o in obras))
    filtro_status_obra = st.sidebar.selectbox(
        "Status da obra", ["Todos"] + status_disponiveis, key="sb_status_obra")

    obras_filtradas = obras if filtro_status_obra == "Todos" \
        else [o for o in obras if o.get("status") == filtro_status_obra]

    if not obras_filtradas:
        st.sidebar.warning("Nenhuma obra com este status."); st.stop()

    # Último update por obra
    try:
        upd = supabase.table("obras_tarefas")\
            .select("obra_id, created_at")\
            .order("created_at", desc=True).execute().data
        ultimo_upd = {}
        for u in upd:
            if u["obra_id"] not in ultimo_upd:
                ultimo_upd[u["obra_id"]] = u.get("created_at","")[:10]
    except:
        ultimo_upd = {}

    def label_obra(o):
        upd_str = ultimo_upd.get(o["id"])
        return f"{o['codigo']} — {o['nome']}" + (f"\n🕐 {upd_str}" if upd_str else "")

    opcoes_obras = {label_obra(o): o["id"] for o in obras_filtradas}
    obra_label   = st.sidebar.selectbox("Obra", list(opcoes_obras.keys()), key="sb_obra")
    obra_id      = opcoes_obras[obra_label]

    # Carregar tarefas
    tarefas = carregar_tarefas(obra_id)

    # Métricas na sidebar
    if tarefas:
        total      = len(tarefas)
        concluidas = sum(1 for t in tarefas if t["status"] == "Concluído")
        atrasadas  = sum(1 for t in tarefas if calcular_farol(t.get("entrega_prevista"), t["status"]) == "🔴")
        impedidas  = sum(1 for t in tarefas if t["status"] == "Impedido")
        pct        = int(concluidas / total * 100) if total else 0
        st.sidebar.progress(pct/100, text=f"Avanço: {pct}%")
        st.sidebar.caption(f"📋 {total} tarefas · ✅ {concluidas} · 🔴 {atrasadas} · 🚧 {impedidas}")

    # Filtros na sidebar
    st.sidebar.markdown("**Filtros**")
    etapas_disp = sorted(set(t.get("etapa") or "—" for t in tarefas if t.get("etapa")))
    f_etapa  = st.sidebar.selectbox("Etapa",  ["Todas"]  + etapas_disp,  key="sb_etapa")
    f_status = st.sidebar.selectbox("Status", ["Todos"]  + STATUS_OPCOES, key="sb_fstatus")
    f_resp   = st.sidebar.selectbox("Resp.",  ["Todos"]  + equipe_nomes,  key="sb_resp")
    f_gut    = st.sidebar.selectbox("GUT",    ["Todos","🔴 Alto (≥75)","🟡 Médio (27–74)","🟢 Baixo (<27)"], key="sb_gut")
    f_conc   = st.sidebar.checkbox("Mostrar Concluídas/N/A", value=False, key="sb_conc")

    # ── ÁREA PRINCIPAL ────────────────────────────────────
    st.header("🏗️ Súmula de Obra")

    # Alertas — ícone clicável
    alertas = carregar_alertas()
    if alertas:
        with st.expander(f"⚠️ {len(alertas)} tarefa(s) vencem em até 2 dias — clique para ver", expanded=False):
            for a in alertas:
                obra_nome = (a.get("obras") or {}).get("nome","?")
                st.markdown(f"- **{obra_nome}** · {a['descricao']} · `{a.get('entrega_prevista','?')}`")

    # ── NOVA TAREFA ───────────────────────────────────────
    with st.expander("➕ Nova tarefa", expanded=False):
        template         = carregar_template()
        etapas_template  = sorted(set(t.get("etapa","") for t in template if t.get("etapa")))
        etapas_obra      = sorted(set(t.get("etapa","") for t in tarefas  if t.get("etapa")))
        etapas_combo     = sorted(set(etapas_template + etapas_obra))
        desc_por_etapa   = {}
        for t in template + tarefas:
            ep = t.get("etapa","")
            desc_por_etapa.setdefault(ep,[]).append(t.get("descricao",""))

        na1,na2,na3 = st.columns(3)
        n_etapa_sel = na1.selectbox("Etapa", ["(nova)"]+etapas_combo, key="n_etapa_sel")
        n_etapa     = na1.text_input("Nome da etapa", key="n_etapa_livre") \
                      if n_etapa_sel == "(nova)" else n_etapa_sel
        n_item      = na2.text_input("Item", key="n_item")
        n_status    = na3.selectbox("Status", STATUS_OPCOES, key="n_status")

        descs = sorted(set(desc_por_etapa.get(n_etapa,[]))) if n_etapa_sel != "(nova)" else []
        if descs:
            n_desc_sel = st.selectbox("Descrição sugerida", ["(escrever)"]+descs, key="n_desc_sel")
            n_desc     = st.text_input("Descrição", key="n_desc_livre") \
                         if n_desc_sel == "(escrever)" else n_desc_sel
        else:
            n_desc = st.text_input("Descrição", key="n_desc_livre2")

        n_obs = st.text_area("Observação", key="n_obs", height=60)

        nr1,nr2,nr3,nr4 = st.columns(4)
        n_r = nr1.selectbox("R — Responsável", equipe_nomes, key="n_r")
        n_a = nr2.selectbox("A — Aprovador",   ["—"]+equipe_nomes, key="n_a")
        n_c = nr3.selectbox("C — Consultado",  ["—"]+equipe_nomes, key="n_c")
        n_i = nr4.selectbox("I — Informado",   ["—"]+equipe_nomes, key="n_i")

        nd1,nd2 = st.columns(2)
        n_inicio  = nd1.date_input("Início previsto",  value=None, key="n_ini", format="DD/MM/YYYY")
        n_entrega = nd2.date_input("Entrega prevista", value=None, key="n_ent", format="DD/MM/YYYY")

        ng1,ng2,ng3,ng4 = st.columns(4)
        n_g = ng1.slider("G",1,5,1,key="n_g")
        n_u = ng2.slider("U",1,5,1,key="n_u")
        n_t = ng3.slider("T",1,5,1,key="n_t")
        ng4.metric("Score GUT", n_g*n_u*n_t)

        if st.button("💾 Criar tarefa", type="primary", key="btn_nova"):
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
                    "gut_gravidade":    n_g,
                    "gut_urgencia":     n_u,
                    "gut_tendencia":    n_t,
                    "gut_score":        n_g*n_u*n_t,
                    "avanco_percent":   0,
                }).execute()
                limpar_cache()
                st.success("✅ Tarefa criada!"); st.rerun()
            else:
                st.warning("Descrição é obrigatória.")

    st.divider()

    # ── SEM TAREFAS ───────────────────────────────────────
    if not tarefas:
        st.info("Esta obra ainda não possui tarefas.")
        template = carregar_template()
        if template and st.button("📋 Criar do Template", type="primary"):
            supabase.table("obras_tarefas").insert([{
                "obra_id":       obra_id,
                "item":          str(t.get("item","")),
                "etapa":         t.get("etapa"),
                "descricao":     t.get("descricao"),
                "status":        t.get("status_padrao") or "A Iniciar",
                "gut_gravidade": 1,"gut_urgencia":1,"gut_tendencia":1,
                "gut_score":     1,"avanco_percent":0,
            } for t in template]).execute()
            limpar_cache(); st.success("✅ Tarefas criadas!"); st.rerun()
        st.stop()

    # ── APLICA FILTROS ────────────────────────────────────
    tf = tarefas
    if not f_conc:   tf = [t for t in tf if t["status"] not in ("Concluído","N/A")]
    if f_etapa  != "Todas":  tf = [t for t in tf if t.get("etapa") == f_etapa]
    if f_status != "Todos":  tf = [t for t in tf if t["status"]    == f_status]
    if f_resp   != "Todos":  tf = [t for t in tf if (t.get("responsavel") or {}).get("nome") == f_resp]
    if "Alto"   in f_gut:    tf = [t for t in tf if (t.get("gut_score") or 1) >= 75]
    elif "Médio" in f_gut:   tf = [t for t in tf if 27 <= (t.get("gut_score") or 1) < 75]
    elif "Baixo" in f_gut:   tf = [t for t in tf if (t.get("gut_score") or 1) < 27]

    st.caption(f"Exibindo **{len(tf)}** tarefa(s) · ordenadas por GUT ↓")

    # ── CABEÇALHO DA TABELA ───────────────────────────────
    hcols = st.columns([1,1,3,2,2,1,1])
    for col, label in zip(hcols, ["GUT","Etapa","Descrição","Resp.","Status","Desv.","Av."]):
        col.markdown(f"<small><b>{label}</b></small>", unsafe_allow_html=True)
    st.divider()

    # ── LINHAS ────────────────────────────────────────────
    for t in tf:
        farol     = calcular_farol(t.get("entrega_prevista"), t["status"])
        desvio    = calcular_desvio(t.get("entrega_prevista"), t.get("entrega_real"), t["status"])
        resp_nome = (t.get("responsavel") or {}).get("nome","—")
        gut_str   = gut_emoji(t.get("gut_score") or 1)
        avanco    = t.get("avanco_percent") or 0

        c1,c2,c3,c4,c5,c6,c7 = st.columns([1,1,3,2,2,1,1])
        c1.markdown(f"<small>{gut_str}</small>",                              unsafe_allow_html=True)
        c2.markdown(f"<small>{t.get('etapa') or '—'}</small>",                unsafe_allow_html=True)
        c3.markdown(f"<small>{farol} {t.get('descricao') or '—'}</small>",    unsafe_allow_html=True)
        c4.markdown(f"<small>{resp_nome}</small>",                             unsafe_allow_html=True)
        c5.markdown(f"<small>{t['status']}</small>",                           unsafe_allow_html=True)
        c6.markdown(f"<small>{desvio}</small>",                                unsafe_allow_html=True)
        c7.markdown(f"<small>{avanco}%</small>",                               unsafe_allow_html=True)

        with st.expander("✏️", expanded=False):
            ea,eb,ec = st.columns(3)
            r_nome = ea.selectbox("R",equipe_nomes,
                index=equipe_nomes.index(resp_nome) if resp_nome in equipe_nomes else 0,
                key=f"r_{t['id']}")
            a_nome = eb.selectbox("A",["—"]+equipe_nomes, key=f"a_{t['id']}")
            c_nome = ec.selectbox("C",["—"]+equipe_nomes, key=f"c_{t['id']}")
            i_nome = ea.selectbox("I",["—"]+equipe_nomes, key=f"i_{t['id']}")

            novo_status = eb.selectbox("Status", STATUS_OPCOES,
                index=STATUS_OPCOES.index(t["status"]) if t["status"] in STATUS_OPCOES else 0,
                key=f"st_{t['id']}")
            novo_avanco = ec.slider("% Avanço",0,100,avanco,step=5,key=f"av_{t['id']}")

            novo_imp = t.get("impedimento") or ""
            if novo_status == "Impedido":
                novo_imp = st.text_input("🚧 Impedimento", value=novo_imp, key=f"imp_{t['id']}")

            fd1,fd2,fd3 = st.columns(3)
            novo_inicio  = fd1.date_input("Início prev.", value=parse_date(t.get("inicio_previsto")),  key=f"ini_{t['id']}", format="DD/MM/YYYY")
            novo_entrega = fd2.date_input("Entrega prev.",value=parse_date(t.get("entrega_prevista")), key=f"ent_{t['id']}", format="DD/MM/YYYY")
            novo_real    = fd3.date_input("Entrega real", value=parse_date(t.get("entrega_real")),     key=f"real_{t['id']}", format="DD/MM/YYYY")

            gg1,gg2,gg3,gg4 = st.columns(4)
            ng = gg1.slider("G",1,5,t.get("gut_gravidade") or 1,key=f"g_{t['id']}")
            nu = gg2.slider("U",1,5,t.get("gut_urgencia")  or 1,key=f"u_{t['id']}")
            nt = gg3.slider("T",1,5,t.get("gut_tendencia") or 1,key=f"te_{t['id']}")
            gg4.metric("Score", ng*nu*nt)

            novo_obs = st.text_area("Observações", value=t.get("observacoes") or "",
                key=f"obs_{t['id']}", height=60)

            if st.button("💾 Salvar", key=f"save_{t['id']}", type="primary"):
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