package br.ufs.dcomp.ChatRabbitMQ;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;

public class ChatCLI {

    private final RabbitMQService service;
    private String destinatarioAtual = "";
    private LineReader leitor;

    public ChatCLI(RabbitMQService service) {
        this.service = service;
    }

    public void iniciar() {
        leitor = LineReaderBuilder.builder().build();

        Thread threadReceba = new Thread(new MensagemRecebida(service, leitor));
        threadReceba.setDaemon(true);
        threadReceba.start();

        while (true) {
            String prompt = destinatarioAtual.isEmpty()
                    ? "<< "
                    : "@" + destinatarioAtual + "<< ";

            String entrada;

            try {
                entrada = leitor.readLine(prompt);
            } catch (Exception e) {
                System.err.println("[Erro ao ler entrada: " + e.getMessage() + "]");
                break;
            }

            entrada = entrada.trim();

            if (entrada.isEmpty()) {
                continue;
            }

            if (entrada.equalsIgnoreCase("/sair")) {
                System.out.println("Encerrando o chat...");
                break;
            }

            if (entrada.startsWith("@")) {
                String novoDestinatario = entrada.substring(1).trim();

                if (novoDestinatario.isEmpty()) {
                    leitor.printAbove("[Erro: informe um usuário após @]");
                    continue;
                }

                destinatarioAtual = novoDestinatario;
                continue;
            }

            if (destinatarioAtual.isEmpty()) {
                leitor.printAbove("[Erro: Defina um destinatário primeiro. Ex: @joao]");
                continue;
            }

            try {
                service.enviarMensagem(destinatarioAtual, entrada);
            } catch (Exception e) {
                leitor.printAbove("[Erro ao enviar mensagem: " + e.getMessage() + "]");
            }
        }
    }
}
