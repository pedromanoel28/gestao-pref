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
                      and t.get("entrega_prevista","") < hoje_iso
                      and t.get("entrega_prevista"))
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

    # ── CARGA ──────────────────────────────────────────────
    @st.cache_data(ttl=300)
    def carregar_custos():
        rows, page, size = [], 0, 1000
        while True:
            resp = supabase.table("custos")\
                .select("data, conta_macro, centro_custos, conta_gerencial, "
                        "cli_fornecedor, valor_global")\
                .range(page * size, (page + 1) * size - 1)\
                .execute()
            rows.extend(resp.data)
            if len(resp.data) < size:
                break
            page += 1
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["data"]         = pd.to_datetime(df["data"], errors="coerce")
        df["valor_global"] = pd.to_numeric(df["valor_global"], errors="coerce").fillna(0)
        df["mes"]          = df["data"].dt.to_period("M").astype(str)
        return df

    def fmt_brl(v):
        if pd.isna(v) or v == 0:
            return "R$ 0"
        sinal = "-" if v < 0 else ""
        return f"{sinal}R$ " + f"{abs(v):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def curva_abc(df_in, col_grupo, col_valor):
        """ABC por valor absoluto — funciona com despesas negativas ou positivas."""
        g = df_in.groupby(col_grupo)[col_valor].sum().reset_index()
        g.columns = ["nome", "valor"]
        g["abs_v"] = g["valor"].abs()
        g = g[g["abs_v"] > 0].copy().sort_values("abs_v", ascending=False).reset_index(drop=True)
        total       = g["abs_v"].sum()
        g["pct"]    = g["abs_v"] / total * 100
        g["cum_pct"]= g["pct"].cumsum()
        g["classe"] = g["cum_pct"].apply(
            lambda x: "A" if x <= 80 else ("B" if x <= 95 else "C"))
        return g.drop(columns="abs_v")

    CORES_ABC = {"A": "#EF5350", "B": "#FFA726", "C": "#66BB6A"}

    def plot_abc(df_abc, key):
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_abc["nome"], y=df_abc["valor"],
            marker_color=[CORES_ABC[c] for c in df_abc["classe"]],
            name="Valor",
            hovertemplate="%{x}<br>R$ %{y:,.0f}<extra></extra>"))
        fig.add_trace(go.Scatter(
            x=df_abc["nome"], y=df_abc["cum_pct"],
            yaxis="y2", mode="lines+markers", name="Acumulado %",
            line=dict(color="#1A237E", width=2),
            hovertemplate="%{y:.1f}%<extra></extra>"))
        fig.update_layout(
            yaxis=dict(tickformat=",.0f", tickprefix="R$ "),
            yaxis2=dict(overlaying="y", side="right",
                        range=[0, 105], ticksuffix="%", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            margin=dict(t=50, b=70), height=360,
            xaxis=dict(tickangle=-40))
        st.plotly_chart(fig, use_container_width=True, key=key)

    def tbl_abc(df_abc, col_nome):
        tbl = df_abc.copy()
        tbl["Valor"] = tbl["valor"].apply(fmt_brl)
        tbl["%"]     = tbl["pct"].apply(lambda x: f"{x:.1f}%")
        tbl["Acum."] = tbl["cum_pct"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(
            tbl[["classe","nome","Valor","%","Acum."]].rename(
                columns={"classe":"Classe","nome":col_nome}),
            use_container_width=True, hide_index=True, height=230)

    # ── CARREGA ────────────────────────────────────────────
    df_custos = carregar_custos()
    st.header("💰 Dashboard de Custos")

    if df_custos.empty:
        st.warning("Nenhum dado encontrado. Importe os arquivos na seção Importador.")
        st.stop()

    # Listas globais de opções
    meses_disp = sorted(df_custos["mes"].dropna().unique(), reverse=True)
    categorias = sorted(df_custos["conta_macro"].fillna("—").unique())
    centros    = sorted(df_custos["centro_custos"].fillna("—").unique())

    # Índice dos meses para navegação
    meses_ord = sorted(df_custos["mes"].dropna().unique())

    # ── FILTRO GLOBAL: Mês ──────────────────────────────────
    fh1, fh2 = st.columns([4, 1])
    sel_mes = fh1.selectbox("Mês de referência", meses_disp, index=0, key="fin_mes")
    if fh2.button("🔄 Atualizar", key="btn_fin_refresh"):
        carregar_custos.clear(); st.rerun()

    idx_atual = meses_ord.index(sel_mes) if sel_mes in meses_ord else len(meses_ord) - 1
    mes_atual = meses_ord[idx_atual]
    mes_ant   = meses_ord[idx_atual - 1] if idx_atual > 0 else None

    df_mes     = df_custos[df_custos["mes"] == mes_atual]
    df_mes_ant = df_custos[df_custos["mes"] == mes_ant] if mes_ant else pd.DataFrame()

    # ── NÍVEL 1: MÉTRICAS (dados completos do mês) ─────────
    total_atual = df_mes["valor_global"].sum()
    total_ant   = df_mes_ant["valor_global"].sum() if not df_mes_ant.empty else 0
    var_pct     = (total_atual - total_ant) / abs(total_ant) * 100 if total_ant else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("💰 Total do mês",  fmt_brl(total_atual))
    m2.metric("📅 Mês anterior",  fmt_brl(total_ant))
    delta_txt = f"{var_pct:+.1f}%"
    m3.metric("📈 Variação",      delta_txt, delta=delta_txt, delta_color="inverse")
    m4.metric("📄 Lançamentos",   f"{len(df_mes):,}".replace(",", "."))

    st.divider()

    # ── NÍVEL 2: EVOLUÇÃO + COMPARATIVO ───────────────────
    st.markdown("#### 📅 Evolução temporal")
    e2a, e2b, e2c = st.columns([3, 3, 1])
    ev_cat    = e2a.multiselect("Categorias", categorias, key="ev_cat",
                                 placeholder="Todas as categorias")
    ev_centro = e2b.multiselect("Centro de custo", centros, key="ev_centro",
                                 placeholder="Todos os centros")
    ev_meses  = e2c.number_input("Meses", 3, 24, 12, key="ev_meses", step=1)

    ev_cat_f    = ev_cat    if ev_cat    else categorias
    ev_centro_f = ev_centro if ev_centro else centros
    df_ev = df_custos[
        df_custos["conta_macro"].isin(ev_cat_f) &
        df_custos["centro_custos"].isin(ev_centro_f)
    ]
    meses_ev = sorted(df_ev["mes"].dropna().unique())
    df_12    = df_ev[df_ev["mes"].isin(meses_ev[-int(ev_meses):])]

    l2a, l2b = st.columns(2)
    with l2a:
        st.subheader("Evolução empilhada")
        df_evol = df_12.groupby(["mes","conta_macro"])["valor_global"].sum().reset_index()
        fig_evol = px.bar(df_evol, x="mes", y="valor_global", color="conta_macro",
                          barmode="stack",
                          labels={"mes":"","valor_global":"R$","conta_macro":""},
                          color_discrete_sequence=px.colors.qualitative.Set2)
        fig_evol.update_layout(
            yaxis_tickformat=",.0f", yaxis_tickprefix="R$ ",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            margin=dict(t=50, b=20), height=340)
        fig_evol.update_traces(
            hovertemplate="%{x}<br>%{fullData.name}<br>R$ %{y:,.0f}<extra></extra>")
        st.plotly_chart(fig_evol, use_container_width=True, key="chart_evol")

    with l2b:
        st.subheader(f"{mes_atual} vs {mes_ant or '—'}")
        comp_a = df_ev[df_ev["mes"] == mes_atual].groupby("conta_macro")["valor_global"].sum().rename("atual")
        comp_b = (df_ev[df_ev["mes"] == mes_ant].groupby("conta_macro")["valor_global"].sum().rename("anterior")
                  if mes_ant else pd.Series(name="anterior", dtype=float))
        df_comp = (pd.concat([comp_a, comp_b], axis=1)
                   .fillna(0).reset_index()
                   .melt("conta_macro", var_name="período", value_name="valor"))
        if mes_ant and not df_comp.empty:
            fig_comp = px.bar(df_comp, x="conta_macro", y="valor", color="período",
                              barmode="group",
                              labels={"conta_macro":"","valor":"R$","período":""},
                              color_discrete_map={"atual":"#1976D2","anterior":"#90CAF9"})
            fig_comp.update_layout(
                yaxis_tickformat=",.0f", yaxis_tickprefix="R$ ",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=50, b=20), height=340, xaxis_tickangle=-25)
            fig_comp.update_traces(hovertemplate="%{x}<br>R$ %{y:,.0f}<extra></extra>")
            st.plotly_chart(fig_comp, use_container_width=True, key="chart_comp")
        else:
            st.info("Sem mês anterior para comparar.")

    st.divider()

    # ── NÍVEL 3: ABC CONTA GERENCIAL + ROSCA ──────────────
    st.markdown("#### 🔍 Concentração de custos")
    a3a, a3b, a3c = st.columns([3, 3, 1])
    abc_cat    = a3a.multiselect("Categorias", categorias, key="abc_cat",
                                  placeholder="Todas as categorias")
    abc_centro = a3b.multiselect("Centro de custo", centros, key="abc_centro",
                                  placeholder="Todos os centros")
    abc_top_n  = a3c.number_input("Top N contas", 5, 100, 20, key="abc_topn", step=5)

    abc_cat_f    = abc_cat    if abc_cat    else categorias
    abc_centro_f = abc_centro if abc_centro else centros
    df_abc_base  = df_mes[
        df_mes["conta_macro"].isin(abc_cat_f) &
        df_mes["centro_custos"].isin(abc_centro_f)
    ]

    l3a, l3b = st.columns(2)
    with l3a:
        st.subheader("Curva ABC — Conta Gerencial")
        abc_cg = curva_abc(df_abc_base, "conta_gerencial", "valor_global").head(int(abc_top_n))
        if not abc_cg.empty:
            plot_abc(abc_cg, key="chart_abc_cg")
            tbl_abc(abc_cg, "Conta Gerencial")
        else:
            st.info("Sem dados para o filtro selecionado.")

    with l3b:
        st.subheader("Distribuição por Categoria")
        df_cat = df_abc_base.groupby("conta_macro")["valor_global"].sum().reset_index()
        df_cat["abs_v"] = df_cat["valor_global"].abs()
        df_cat = df_cat[df_cat["abs_v"] > 0]
        if not df_cat.empty:
            fig_pie = go.Figure(go.Pie(
                labels=df_cat["conta_macro"],
                values=df_cat["abs_v"],
                hole=0.42,
                textinfo="label+percent",
                textposition="outside",
                hovertemplate="%{label}<br>R$ %{value:,.0f}<br>%{percent}<extra></extra>",
                marker=dict(colors=px.colors.qualitative.Set2)))
            fig_pie.update_layout(
                showlegend=False, margin=dict(t=30, b=30, l=30, r=30), height=450)
            st.plotly_chart(fig_pie, use_container_width=True, key="chart_pie")
        else:
            st.info("Sem dados para o filtro selecionado.")

    st.divider()

    # ── NÍVEL 4: TOP N FORNECEDORES + ABC ─────────────────
    st.markdown("#### 🏭 Análise de fornecedores")
    f4a, f4b, f4c = st.columns([3, 3, 1])
    forn_cat    = f4a.multiselect("Categorias", categorias, key="forn_cat",
                                   placeholder="Todas as categorias")
    forn_centro = f4b.multiselect("Centro de custo", centros, key="forn_centro",
                                   placeholder="Todos os centros")
    forn_top_n  = f4c.number_input("Top N", 5, 100, 15, key="forn_topn", step=5)

    forn_cat_f    = forn_cat    if forn_cat    else categorias
    forn_centro_f = forn_centro if forn_centro else centros
    df_forn_base  = df_mes[
        df_mes["conta_macro"].isin(forn_cat_f) &
        df_mes["centro_custos"].isin(forn_centro_f)
    ]

    l4a, l4b = st.columns(2)
    with l4a:
        st.subheader(f"Top {int(forn_top_n)} Fornecedores")
        # ascending=True → mais negativo (maior custo) aparece primeiro
        df_forn = (df_forn_base.groupby("cli_fornecedor")["valor_global"]
                   .sum()
                   .sort_values(ascending=True)
                   .head(int(forn_top_n))
                   .reset_index())
        df_forn.columns = ["fornecedor", "valor"]
        if not df_forn.empty:
            fig_forn = go.Figure(go.Bar(
                x=df_forn["valor"],
                y=df_forn["fornecedor"],
                orientation="h",
                marker_color="#42A5F5",
                text=df_forn["valor"].apply(fmt_brl),
                textposition="outside",
                hovertemplate="%{y}<br>R$ %{x:,.0f}<extra></extra>"))
            fig_forn.update_layout(
                xaxis=dict(tickformat=",.0f", tickprefix="R$ "),
                yaxis=dict(autorange="reversed"),
                margin=dict(t=20, b=10, r=200),
                height=max(300, int(forn_top_n) * 30))
            st.plotly_chart(fig_forn, use_container_width=True, key="chart_forn")

    with l4b:
        st.subheader("Curva ABC — Fornecedores")
        abc_forn = curva_abc(df_forn_base, "cli_fornecedor", "valor_global")
        if not abc_forn.empty:
            # Esconde labels do eixo X (muitos fornecedores) mas mantém hover
            fig_abc_f = go.Figure()
            fig_abc_f.add_trace(go.Bar(
                x=abc_forn["nome"], y=abc_forn["valor"],
                marker_color=[CORES_ABC[c] for c in abc_forn["classe"]],
                name="Valor",
                hovertemplate="%{x}<br>R$ %{y:,.0f}<extra></extra>"))
            fig_abc_f.add_trace(go.Scatter(
                x=abc_forn["nome"], y=abc_forn["cum_pct"],
                yaxis="y2", mode="lines+markers", name="Acumulado %",
                line=dict(color="#1A237E", width=2),
                hovertemplate="%{y:.1f}%<extra></extra>"))
            fig_abc_f.update_layout(
                yaxis=dict(tickformat=",.0f", tickprefix="R$ "),
                yaxis2=dict(overlaying="y", side="right",
                            range=[0, 105], ticksuffix="%", showgrid=False),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                margin=dict(t=50, b=20), height=360,
                xaxis=dict(showticklabels=False))
            st.plotly_chart(fig_abc_f, use_container_width=True, key="chart_abc_forn")
            tbl_abc(abc_forn, "Fornecedor")
        else:
            st.info("Sem dados para o filtro selecionado.")