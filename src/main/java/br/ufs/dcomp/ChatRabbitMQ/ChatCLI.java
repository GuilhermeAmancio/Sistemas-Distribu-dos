package br.ufs.dcomp.ChatRabbitMQ;

import java.util.Scanner;

public class ChatCLI {

    private final RabbitMQService service;
    private String destinatarioAtual = "";

    public ChatCLI(RabbitMQService service) {
        this.service = service;
    }

    public void iniciar() {
        Scanner scanner = new Scanner(System.in);
        String usuarioLogado = service.getUsernameUsuario();

        Thread threadReceba = new Thread(new MensagemRecebida(service));

        threadReceba.start();

        // 1. Loop principal de leitura do teclado
        while (true) {
            // Formata o prompt: "User:" se não houver destinatário ou "@destinatario>>"
            String prompt = destinatarioAtual.isEmpty() 
                    ? "<<: " 
                    : "@" + destinatarioAtual + "<< ";

            System.out.print(prompt);

            if (!scanner.hasNextLine()) {
                break;
            }

            String entrada = scanner.nextLine().trim();

            if (entrada.isEmpty()) {
                continue;
            }

            // Comando de saída
            if (entrada.equalsIgnoreCase("/sair")) {
                System.out.println("Encerrando o chat...");
                break;
            }

            // 2. Troca de destinatário (Ex: @joao<<)
            if (entrada.startsWith("@") && entrada.endsWith("<<")) {
                // Remove o '@' do início e o '<<' do fim
                destinatarioAtual = entrada.substring(1, entrada.length() - 2).trim();
                continue;
            }

            // 3. Validação e envio de mensagem
            if (destinatarioAtual.isEmpty()) {
                System.out.println("[Erro: Defina um destinatário primeiro. Ex: @joao<<]");
            } else {
                try {
                    // Chama a função da Pessoa 1 para enviar via RabbitMQ
                    service.enviarMensagem(destinatarioAtual, entrada);
                } catch (Exception e) {
                    System.err.println("[Erro ao enviar mensagem: " + e.getMessage() + "]");
                }
            }
        }
    }
}