import grpc
import time
import os 
import psycopg2
from concurrent import futures

import veiculos_pb2
import veiculos_pb2_grpc

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "frota_veiculos")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")


#classe de acesso ao banco de dados
class VeiculosDB:
    def __init__(self):
        self._conn = None
        self._cursor = None
        self._connect()

    def _connect(self):
        """Tenta estabelecer a conexão com o PostgreSQL"""
        max_retries = 5
        retry_delay_seconds = 5

        for i in range(max_retries):
            try:
                print(f"Tentando conectar ao PostgreSQL em: {DB_HOST}...")
                self._conn = psycopg2.connect(
                    host=DB_HOST,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                self._conn.autocommit = True
                self._cursor = self._conn.cursor()
                print("Conexão com o PostgreSQL estabelecida com sucesso!")

                self._setup_db()
                return
            except psycopg2.OperationalError as e:
                print(f"Erro de conexão com o DB: {e}. Tentativa {i + 1}/{max_retries}.")
                if i < max_retries - 1:
                    time.sleep(retry_delay_seconds)
                else:
                    raise ConnectionError("Falha ao conectar ao PostgreSQL após várias tentativas")
    def _setup_db(self):
        """Cria a tabela de veiculos se ela não existir."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS veiculos(
            id SERIAL PRIMARY KEY,
            placa VARCHAR(10) UNIQUE NOT NULL,
            modelo VARCHAR(100) NOT NULL,
            ano INTEGER
        );
        """
        self._cursor.execute(create_table_query)
        self._cursor.execute("SELECT COUNT(*) FROM veiculos;")
        count = self._cursor.fetchone()[0]

        if count == 0:
            print("Inserindo dados iniciais na tabela 'veiculos'...")
            self._cursor.execute(
                "INSERT INTO veiculos (placa, modelo, ano) VALUES (%s, %s, %s) ON CONFLICT (placa) DO NOTHING;",
                ('ABC-1234', 'Fusion', 2018)
            )
            self._cursor.execute(
                "INSERT INTO veiculos (placa, modelo, ano) VALUES (%s, %s, %s) ON CONFLICT (placa) DO NOTHING;",
                ('DEF-5678', 'Civic', 2020)
            )
            print("Dados de teste inseridos.")

        self._conn.commit()

    def fetch_all(self):
        """Busca todos os veiculos no banco."""
        self._cursor.execute("SELECT id, placa, modelo, ano FROM veiculos;")
        return self._cursor.fetchall()
    
    def fetch_by_placa(self, placa):
        """Busca um veiculo pela placa"""
        self._cursor.execute("SELECT id, placa, modelo, ano FROM veiculos WHERE placa = %s;", (placa,))        
        return self._cursor.fetchone()



class GestaoVeiculosServicer(veiculos_pb2_grpc.GestaoVeiculosServicer):
    def __init__(self):
        """
        Inicializa o serviço, criando uma instância da classe de acesso ao banco de dados.
        """

        self.db = VeiculosDB()

    def ListarTodos(self, request, context):
        """
        Implementa o RPC ListarTodos.
        Retorna uma lista de todos os veículos cadastrados.
        """
        veiculos_tuples = self.db.fetch_all()

        veiculos_grpc = []

        for v_id, placa, modelo, ano in veiculos_tuples:

            veiculos_grpc.append(
                veiculos_pb2.Veiculo(id=str(v_id), placa = placa, modelo = modelo, ano = ano)
            )

        return veiculos_pb2.ListaVeiculos(veiculos=veiculos_grpc)
    
    def BuscarPorPlaca(self, request, context):
        """
        implementa o RPC BuscarPorPlaca.
        Busca um veículo pela Placa (usado pelo microserviço de manutenções).
        """
        veiculo_tuple = self.db.fetch_by_placa(request.placa)

        if veiculo_tuple:
            v_id, placa, modelo, ano = veiculo_tuple
            return veiculos_pb2.Veiculo(
                id = str(v_id),
                placa = placa,
                modelo = modelo,
                ano = ano
            )
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Veículo com placa {request.placa} não encontrado.")
            return veiculos_pb2.Veiculo()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    veiculos_pb2_grpc.add_GestaoVeiculosServicer_to_server(
        GestaoVeiculosServicer(), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()

    print("Microserviço de Gestão de Veiculos rodando na porta 50051.")

    try:
        loop_counter = 0
        while True:
            loop_counter += 1
            print(f"Servidor gRPC ativo. Loop de manutenção: {loop_counter}")
            time.sleep(5)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()        




