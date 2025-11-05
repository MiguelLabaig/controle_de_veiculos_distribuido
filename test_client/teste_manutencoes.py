import grpc
import manutencoes_pb2
import manutencoes_pb2_grpc
import veiculos_pb2
import veiculos_pb2_grpc

TARGET_HOST_MANUTENCOES = 'localhost:50052'
TARGET_HOST_VEICULOS = 'localhost:50051'

def run_test():
    channel_manutencoes = grpc.insecure_channel(TARGET_HOST_MANUTENCOES)
    stub_manutencoes = manutencoes_pb2_grpc.GestaoManutencoesStub(channel_manutencoes)
    print(f"\n--- Cliente de Teste conectado ao Microserviço de Manutenções ({TARGET_HOST_MANUTENCOES}) ---")

    channel_veiculos = grpc.insecure_channel(TARGET_HOST_VEICULOS)
    stub_veiculos = veiculos_pb2_grpc.GestaoVeiculosStub(channel_veiculos)

    placa_valida = "XVZ-0000"

    try:
        response_list = stub_veiculos.ListarTodos(veiculos_pb2.Empty())
        if response_list.items:
            placa_valida = response_list.items[0].placa
            print(f"Placa válida encontrada no MS Veículos: {placa_valida}")

        else:
            print(f"AVISO: Nenhuma placa encontrada no MS Veículos. Usando placa de default: {placa_valida}")

    except grpc.RpcError as e:
        print(f"ERRO: Falha ao conectar/acessar MS Veículos: {e.details()}")
        print(f"Continuando o teste de Manutenção com a placa default.")

    print("\n--- TESTE 1: Criar Manutenção com Placa Válida ---")
    try:
        manutencao_request = manutencoes_pb2.ManutencaoRequest(
            placa_veiculo = placa_valida,
            descricao = "Troca de óleo e filtros."
        )
        response = stub_manutencoes.CriarManutencao(manutencao_request)
        print(f"SUCESSO: Manutenção criada!")
        print(f"ID: {response.id}, Placa: {response.placa_veiculo}, Status: {response.status}")
    except grpc.RpcError as e:
        print(f"FALHA INESPERADA: Erro RPC recebido: {e.code().name}")
        print(f"Detalhes: {e.details()}")

    print("\n--- TESTE 2: Criar Manutenção com Placa INVÁLIDA ---")
    try:
        manutencao_request_invalida = manutencoes_pb2.ManutencaoRequest(
            placa_veiculo="ABC-9999",
            descricao = "Reparo de emergência."
        )
        stub_manutencoes.CriarManutencao(manutencao_request_invalida)
    except grpc.RpcError as e:
        print(f"SUCESSO: Tentativa bloqueada!")
        print(f"ERRO DE VALIDAÇÃO (Esperando): {e.details()}")

if __name__ == "__main__":
    run_test()