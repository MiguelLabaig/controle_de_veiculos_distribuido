import grpc
import time
import os
import psycopg2
from concurrent import futures
from dotenv import load_dotenv

import manutencoes_pb2
import manutencoes_pb2_grpc

import veiculos_pb2
import veiculos_pb2_grpc


DB_HOST = os.getenv("MANUTENCOES_DBHOST", "db_manutencoes")
DB_NAME = os.getenv("MANUTENCOES_DB_NAME", "manutencoes_db")
DB_USER = os.getenv("MANUTENCOES_DB_USER", "admin")
DB_PASSWORD = os.getenv("MANUTENCOES_DB_PASSWORD", "admin")

VEICULOS_SERVICE_HOST = os.getenv("VEICULOS_HOST", "micro_veiculos:500051")

class ManutencoesDB:
    def __init__(self):
        self._connect()
    
    def _connect(self, max_retries=5):
        """Tenta conectar ao PostgreSQL com retry."""
        for i in range(max_retries):
            try:
                print(f"Tentando conectar ao PostgreSQL de Manutenções em: {DB_HOST} ({i+1}/{max_retries})...")
                self._conn = psycopg2.connect(
                    host=DB_HOST,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                self._conn.autocommit = True
                self._cursor = self._conn.cursor()
                print("Conexão com o PostgreSQL de Manutenções estabelecida com sucesso!")
                self._setup_db()
                return
            except psycopg2.OperationalError as e:
                print(f"Erro de conexão: {e}. Aguardando 5 segundos para tentar novamente.")
                # CORREÇÃO 2: Usar time.sleep, não self.sleep
                time.sleep(5) 
        # O loop falhou após todas as tentativas
        raise Exception("Falha ao conectar ao PostgreSQL após várias tentativas.")
    
    def _setup_db(self):
        """cria a tabels de manutenções se ela não existir."""

        create_table_query = """
        CREATE TABLE IF NOT EXISTS manutencoes(
            id SERIAL PRIMARY KEY,
            id_veiculo VARCHAR(100) NOT NULL,
            placa_veiculo VARCHAR(10) NOT NULL,
            descricao VARCHAR(255) NOT NULL,
            status VARCHAR(50) DEFAULT 'PENDENTE'
        );
        """
        self._cursor.execute(create_table_query)
        # self._conn.commit()
        print(f"Tabela 'manutencoes' verificada/criada.")

        pass

    def create_manutencao(self, id_veiculo, placa_veiculo, descricao):
        insert_query = """

        INSERT INTO manutencoes (id_veiculo, placa_veiculo, descricao)
        VALUES (%s, %s, %s) RETURNING id, id_veiculo, placa_veiculo, descricao, status;

        """

        self._cursor.execute(insert_query, (id_veiculo, placa_veiculo, descricao))
        # self._connect.commit()
        result = self._cursor.fetchone()
        return result
    
    def list_all_manutencoes(self):
        query = "SELECT id, id_veiculo, placa_veiculo, descricao, status FROM manutencoes;"
        self._cursor.execute(query)
        results = self._cursor.fetchall()
        return results
    
    def get_manutencao_by_id(self, manutencao_id):
        """Busca uma manutenção pelo ID."""
        query = "SELECT id, id_veiculo, placa_veiculo, descricao, status FROM manutencoes WHERE id = %s;"
        
        self._cursor.execute(query, (str(manutencao_id)))
        result = self._cursor.fetchone()

        return result
        
    

class GestaoManutencoesServicer(manutencoes_pb2_grpc.GestaoManutencoesServicer):
    def __init__(self):
        self.db = ManutencoesDB()
        self.veiculos_channel = grpc.insecure_channel(VEICULOS_SERVICE_HOST)
        self.veiculos_stub = veiculos_pb2_grpc.GestaoVeiculosStub(self.veiculos_channel)
        print(f"Cliente gRPC para Veículos inicializado em: {VEICULOS_SERVICE_HOST}")

    def CriarManutencao(self, request, context):
        placa = request.placa_veiculo
        descricao = request.descricao

        try:
            print(f"Chamando MS Veiculos para obter ID para placa: {placa}")
            veiculo_response = self.veiculos_stub.BuscarPorPlaca(veiculos_pb2.VeiculoPlaca(placa=placa))
            id_veiculo = veiculo_response.id
            print(f"ID do Veiculo encontrado: {id_veiculo}")

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Veículo com placa {placa} não encontrado. Manutenção não pode ser criada.")
                return manutencoes_pb2.Manutencao()
            context.set_code(e.code())
            context.set_details(f"Erro ao comunicar com o MS Veiculos: {e.details()}")
            return manutencoes_pb2.Manutencao()
        try:
            db_result = self.db.create_manutencao(id_veiculo, placa, descricao)

            m_id, m_id_veiculo, m_placa_veiculo, m_descricao, m_status = db_result

            return manutencoes_pb2.Manutencao(
                id=str(m_id),
                id_veiculo=m_id_veiculo,
                placa_veiculo=m_placa_veiculo,
                descricao=m_descricao,
                status=m_status

            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Erro interno ao salvar manutenção: {str(e)}")
            return manutencoes_pb2.Manutencao()
        

    def ListarManutencoes(self, request, context):
        try:
            db_results = self.db.list_all_manutencoes()
            lista_manutencoes = manutencoes_pb2.ListaManutencoes()
            for m_id, id_veiculo, placa_veiculo, descricao, status in db_results:
                lista_manutencoes.manutencoes.append(
                    manutencoes_pb2.Manutencao(
                        id = str(m_id),
                        id_veiculo = id_veiculo,
                        placa_veiculo = placa_veiculo,
                        descricao = descricao,
                        status = status
                    )
                )

            return lista_manutencoes
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Erro interno ao listar manutenções: {str(e)}")
            return manutencoes_pb2.ListarManutencoes()
        
    def BuscarPorId(self, request, context):
        try:
            m_id = int(request.id)
            db_result = self.db.get_manutencao_by_id(m_id)

            if not db_result:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Manutenção com ID {m_id} não encontrada.")
                return manutencoes_pb2.Manutencao()
            
            m_id, id_veiculo, placa_veiculo, descricao, status = db_result

            return manutencoes_pb2.Manutencao(
                id=str(m_id),
                id_veiculo=id_veiculo,
                placa_veiculo=placa_veiculo,
                descricao=descricao,
                status=status
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Erro interno ao buscar manutenção por ID: {str(e)}")
            return manutencoes_pb2.Manutencao()
        

    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    manutencoes_pb2_grpc.add_GestaoManutencoesServicer_to_server(
        GestaoManutencoesServicer(), server
    )
    server.add_insecure_port('[::]:50052')
    server.start()

    print(f"Microserviço de Gestão de Manutenções rodando na porta 50052.")

    try:
        loop_counter = 0
        while True:
            loop_counter += 1
            print(f"MS Manutenções ativo. Loop de manutenção: {loop_counter}")
            time.sleep(5)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()

