# Civil Gestão — Contexto do Projeto

## Stack atual
- **Linguagem:** Python 3.12
- **Framework:** Streamlit
- **Arquivo principal:** main.py
- **Banco de dados:** Supabase (PostgreSQL)
- **Hospedagem:** Streamlit Cloud
- **Repositório:** github.com/pedromanoel28/gestao-pref

## Credenciais Supabase

## Estrutura do Banco

| Tabela | Função |
|--------|--------|
| obras | Cadastro de obras (codigo, nome, status) |
| obras_tarefas | Tarefas RACI — obra_id NULL = tarefa extra |
| equipe | Colaboradores (10 pessoas) |
| template_jornada | Etapas padrão para novas obras |
| rotinas | Checklist diário por setor |
| custos | Lançamentos contábeis (~8k registros) |
| notas_fiscais | NFs por obra (~2.8k registros) |
| folha | Folha de pagamento (~1.1k registros) |
| producao_fabricacao | Peça a peça fabricação (~76k registros) |
| producao_transporte | Peça a peça expedição (~72k registros) |
| producao_montagem | Peça a peça montagem (~10k registros) |

## Colunas especiais em obras_tarefas
- gut_gravidade, gut_urgencia, gut_tendencia, gut_score (Matriz GUT)
- avanco_percent (0–100)
- impedimento (texto — só preenchido quando status = Impedido)
- origem (texto — identifica contexto das tarefas extras)
- obra_id NULL = tarefa extra (não vinculada a obra)
- responsavel_id, aprovador_id, consultado_id, informado_id → FK equipe

## Equipe (10 pessoas)
Pedro Manoel Santos, Danielle Rayol, Victor Passos, Raul Odilon,
Rodrigo Paranhos, Daiane Oliveira, Karine Barros, Nathalia Brito,
Alexsander Cerqueira, Heber Amaral

## Páginas implementadas ✅
- **🏗️ Gestão de Obras** — Súmula completa com:
  - Seleção de obra + visão global (Todas as obras)
  - Matriz GUT, farol de prazo, desvio em dias, % avanço
  - Aba Extras (obra_id NULL)
  - Filtros: etapa, status, responsável, GUT
  - st.dataframe nativo com seleção de linha para edição
  - Criar do Template
- **👥 Equipe** — CRUD completo
- **📥 Importador** — 10 rotas de importação CSV

## Páginas em construção 🚧
- 🏭 Produção (dashboard fabricação, transporte, montagem)
- 💰 Financeiro (NFs, custos)
- 👷 Folha / RH
- 👤 Reunião 1:1
- 📋 Gestão à Vista (rotinas + checklist)
- 📊 Gestão à Vista

## Convenções obrigatórias do código
- Cache: @st.cache_data(ttl=60) em todas as funções de leitura
- Sempre chamar limpar_cache() após qualquer escrita no banco
- Status possíveis: A Iniciar, Em Andamento, Impedido, Concluído, N/A
- Datas: sempre isoformat() antes de enviar ao Supabase
- IDs: sempre int() antes de enviar (evita float no bigint)
- Nunca usar NaN — usar None para valores vazios

## Roadmap futuro
- Login por usuário (Supabase Auth)
- Permissões por setor
- Notificações de prazo vencido
- Histórico de alterações (audit log)
- Integração Trello/Plannix
- App mobile (PWA)