package br.ufs.dcomp.ChatRabbitMQ;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;

public class ChatCLI {

    private final RabbitMQService service;
    private String destinatarioAtual = "";
    private LineReader leitor;
    private final GroupService groupService = new GroupService();

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

            if(entrada.startsWith("!")) {
                String[] partes = entrada.split(" ", 3);

                switch (partes[0]) {
                    case "!addGroup":
                        if (partes.length == 2) {
                            leitor.printAbove(groupService.criarGrupo(partes[1], service.getUsernameUsuario()));
                        } else {
                            leitor.printAbove("Metodo de uso: !addGroup <grupo>");
                        }
                        break;

                    case "!addUser":
                        if (partes.length == 3) {
                            leitor.printAbove(groupService.adicionarUsuario(partes[1], partes[2]));
                        } else {
                            leitor.printAbove("Metodo de uso: !addUser <usuario> <grupo>");
                        }
                        break;

                    case "!removeUser":
                        if (partes.length == 3) {
                            leitor.printAbove(groupService.removerUsuario(partes[1], partes[2]));
                        } else {
                            leitor.printAbove("Metodo de uso: !removeUser <usuario> <grupo>");
                        }
                        break;

                    case "!removeGroup":
                        if (partes.length == 2) {
                            leitor.printAbove(groupService.removerGrupo(partes[1]));
                        } else {
                            leitor.printAbove("Metodo de uso: !removeGroup <grupo>");
                        }
                        break;

                    default:
                        leitor.printAbove("Comando desconhecido");
                }

                continue;
            }

            if (entrada.equalsIgnoreCase("/sair")) {
                leitor.printAbove("Encerrando o chat...");
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
