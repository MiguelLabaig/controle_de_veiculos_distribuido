import grpc
import veiculos_pb2
import veiculos_pb2_grpc

TARGET_HOST="localhost:50051"

def run_test():
    """Simula as chamadas gRPC para o Microserviço de Gestão de Veiculos."""

    with grpc.insecure_channel(TARGET_HOST) as channel:
        stub = veiculos_pb2_grpc.GestaoVeiculosStub(channel)
        print(f"--- Cliente de Teste conectado ao Microserviço de Veículos ({TARGET_HOST}) ---")

        placa_sucesso = 'ABC-1234'
        placa_falha = 'XXX-9999'

        for placa in [placa_sucesso, placa_falha]:
            print(f"\n [TESTE 1] Chamando BuscarPorPlaca com Placa: {placa}")

            try:
                response = stub.BuscarPorPlaca(veiculos_pb2.VeiculoPlaca(placa = placa))
                print(f"SUCESSO: Veículo encontrado!")
                print(f" > ID (Chave de Referência): {response.id}")
                print(f" > Modelo: {response.modelo}")
                print(f" > Ano: {response.ano}")
                
            except grpc.RpcError as e:
                print(f"FALHA: Erro RPC recebido: {e.code().name}")
                print(f" > Detalhes: {e.details()}")


        print("\n[TESTE 2] Chamado ListarTodos")
        try:
            response_list = stub.ListarTodos(veiculos_pb2.Empty())

            print(f"SUCESSO: Total de veículos encontrados: {len(response_list.items)}")
            for veiculo in response_list.items:
                print(f" - ID: {veiculo.id}, Placa: {veiculo.placa}, Modelo: {veiculo.modelo}")

        except grpc.RpcError as e:

            print(f"FALHA: Erro RPC recebido: {e.code().name} - {e.details()}")

if __name__ == '__main__':
    run_test()