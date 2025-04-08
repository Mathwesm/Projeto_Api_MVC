import pandas as pd
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, DATETIME, text, inspect
from sqlalchemy.exc import IntegrityError
from Model.def_url import chamar_api_myfinance
from datetime import datetime
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Configuração de Caminhos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 2. Obtenção de Dados da API
def obter_dados_api():
    try:
        url = "https://myfin-financial-management.bubbleapps.io/api/1.1/obj/transactions"
        token = os.getenv("API_TOKEN")
        headers = {"Authorization": f"Bearer {token}"}

        logger.info("Obtendo dados da API...")
        dados = chamar_api_myfinance(url)
        df = pd.DataFrame(dados)
        logger.info(f"Total de registros obtidos: {len(df)}")

        # Conversão de datas
        colunas_data = ['Modified Date', 'Created Date', 'estimated_date', 'payment_date']
        for coluna in colunas_data:
            df[coluna] = pd.to_datetime(df[coluna], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

        return df

    except Exception as erro:
        logger.error(f"Erro ao obter dados da API: {erro}")
        raise

# 3. Configuração do Banco de Dados
def configurar_conexao_banco():
    try:
        configuracoes_banco = {
            'usuario': os.getenv("DB_USER"),
            'senha': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'banco': os.getenv("DB_NAME")
        }

        engine = create_engine(
            f'mysql+pymysql://{configuracoes_banco["usuario"]}:{configuracoes_banco["senha"]}@'
            f'{configuracoes_banco["host"]}/{configuracoes_banco["banco"]}',
            pool_pre_ping=True,
            pool_recycle=3600
        )

        # Testar conexão
        with engine.connect() as conexao:
            logger.info(f"Conexão bem-sucedida com o banco: {configuracoes_banco['banco']}")

        return engine

    except Exception as erro:
        logger.error(f"Erro na conexão com o banco: {erro}")
        raise

# 4. Verificação da Estrutura da Tabela
def verificar_estrutura_tabela(engine):
    metadata = MetaData()

    tabela_transacoes = Table('transactions', metadata,
        Column('Modified Date', DATETIME),
        Column('Created Date', DATETIME),
        Column('Created By', String(255)),
        Column('estimated_date', DATETIME),
        Column('recipient_ref', String(255)),
        Column('status', String(255)),
        Column('amount', DECIMAL(10, 2)),
        Column('year_ref', Integer),
        Column('payment_date', DATETIME),
        Column('OS_type-transaction', String(255)),
        Column('user_ref', String(255)),
        Column('cod_ref', String(255)),
        Column('month_ref', Integer),
        Column('OS_frequency-type', String(255)),
        Column('_id', String(255), primary_key=True)
    )

    inspetor = inspect(engine)

    if not inspetor.has_table('transactions'):
        logger.info("Criando nova tabela transactions...")
        metadata.create_all(engine)
    else:
        logger.info("Verificando estrutura da tabela existente...")
        colunas_existentes = {coluna['name'] for coluna in inspetor.get_columns('transactions')}
        colunas_definidas = {coluna.name for coluna in tabela_transacoes.columns}

        # Adicionar colunas faltantes
        with engine.begin() as conexao:
            for coluna in (colunas_definidas - colunas_existentes):
                definicao_coluna = next(col for col in tabela_transacoes.columns if col.name == coluna)
                tipo_coluna = definicao_coluna.type.compile(engine.dialect)
                logger.info(f"Adicionando coluna: {coluna} ({tipo_coluna})")
                conexao.execute(text(f"ALTER TABLE transactions ADD COLUMN `{coluna}` {tipo_coluna}"))

    return tabela_transacoes

# 5. Inserção de Dados
def inserir_dados(engine, df, nome_tabela='transactions'):
    try:
        # Filtra colunas existentes na tabela
        inspetor = inspect(engine)
        colunas_tabela = {col['name'] for col in inspetor.get_columns(nome_tabela)}
        colunas_df = set(df.columns)
        colunas_validas = list(colunas_df & colunas_tabela)

        if not colunas_validas:
            raise ValueError("Nenhuma coluna correspondente encontrada entre DataFrame e tabela")

        df_filtrado = df[colunas_validas]

        # Tentativa de inserção em lote
        logger.info("Iniciando inserção em lote...")
        tempo_inicio = datetime.now()

        df_filtrado.to_sql(
            name=nome_tabela,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        duracao = (datetime.now() - tempo_inicio).total_seconds()
        logger.info(f"Inserção em lote concluída. {len(df)} registros em {duracao:.2f} segundos")

    except Exception as erro_lote:
        logger.warning(f"Falha na inserção em lote: {erro_lote}. Iniciando inserção individual...")

        estatisticas = {
            'sucesso': 0,
            'duplicados': 0,
            'erros': 0,
            'exemplos_erros': []
        }

        for _, registro in df_filtrado.iterrows():
            try:
                registro.to_frame().T.to_sql(
                    name=nome_tabela,
                    con=engine,
                    if_exists='append',
                    index=False
                )
                estatisticas['sucesso'] += 1

            except IntegrityError as e:
                if 'Duplicate entry' in str(e.orig):
                    estatisticas['duplicados'] += 1
                else:
                    estatisticas['erros'] += 1
                    estatisticas['exemplos_erros'].append(str(e))
            except Exception as e:
                estatisticas['erros'] += 1
                estatisticas['exemplos_erros'].append(str(e))

        logger.info("\nResumo da inserção:")
        logger.info(f"- Sucesso: {estatisticas['sucesso']}")
        logger.info(f"- Duplicados: {estatisticas['duplicados']}")
        logger.info(f"- Erros: {estatisticas['erros']}")

        if estatisticas['erros'] > 0:
            logger.warning(f"Exemplos de erros: {estatisticas['exemplos_erros'][:3]}")

# Função Principal
def main():
    try:
        # 1. Obter dados
        df = obter_dados_api()

        # 2. Configurar banco
        engine = configurar_conexao_banco()

        # 3. Verificar tabela
        verificar_estrutura_tabela(engine)

        # 4. Inserir dados
        inserir_dados(engine, df)

        logger.info("Processo concluído com sucesso!")

    except Exception as e:
        logger.error(f"Erro no processo principal: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
