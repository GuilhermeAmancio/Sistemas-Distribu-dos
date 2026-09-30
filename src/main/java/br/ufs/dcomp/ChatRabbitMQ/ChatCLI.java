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
        String usuarioLogado = service.getUsernameUsuario();

        leitor = LineReaderBuilder.builder().build();

        
        //esta thread basicamente vai servir para que o programa consiga enviar mensagens pela thread principal
        //mas também receber mensagens por esta thread secundária
        Thread threadReceba = new Thread(new MensagemRecebida(service, leitor));

        threadReceba.setDaemon(true);
        threadReceba.start();

        // 1. Loop principal de leitura do teclado
        while (true) {
            // Formata o prompt: "User:" se não houver destinatário ou "@destinatario>>"
            String prompt = destinatarioAtual.isEmpty() 
                    ? "<<: " 
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
               leitor.printAbove("[Erro: Defina um destinatário primeiro. Ex: @joao<<]");
               continue;
            } 
                
            try {
                // Chama a função da Pessoa 1 para enviar via RabbitMQ
                service.enviarMensagem(destinatarioAtual, entrada);
            } catch (Exception e) {
                leitor.printAbove("[Erro ao enviar mensagem: " + e.getMessage() + "]");
            }
            
        }
    }
}