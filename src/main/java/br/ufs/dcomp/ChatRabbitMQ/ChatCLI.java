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
                    : destinatarioAtual + "<< ";

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

            if (entrada.startsWith("!")) {
                String[] partes = entrada.split(" ", 3);

                try {
                    switch (partes[0]) {
                        case "!addGroup":
                            if (partes.length == 2) {
                                String nomeGrupo = partes[1];
                                String resposta = groupService.criarGrupo(nomeGrupo, service.getUsernameUsuario());
                                service.criarExchangeGrupo(nomeGrupo);
                                service.adicionarUsuarioAoExchange(service.getUsernameUsuario(), nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Método de uso: !addGroup <grupo>");
                            }
                            break;

                        case "!addUser":
                            if (partes.length == 3) {
                                String usuarioAlvo = partes[1];
                                String nomeGrupo = partes[2];
                                String resposta = groupService.adicionarUsuario(usuarioAlvo, nomeGrupo);
                                service.adicionarUsuarioAoExchange(usuarioAlvo, nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Método de uso: !addUser <usuário> <grupo>");
                            }
                            break;

                        case "!removeUser":
                            if (partes.length == 3) {
                                String usuarioAlvo = partes[1];
                                String nomeGrupo = partes[2];
                                String resposta = groupService.removerUsuario(usuarioAlvo, nomeGrupo);
                                // Chama o método do RabbitMQService para desfazer o binding no broker
                                service.removerUsuarioDoExchange(usuarioAlvo, nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Método de uso: !removeUser <usuário> <grupo>");
                            }
                            break;

                        case "!removeGroup":
                            if (partes.length == 2) {
                                String nomeGrupo = partes[1];
                                String resposta = groupService.removerGrupo(nomeGrupo);
                                service.removerExchangeGrupo(nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Método de uso: !removeGroup <grupo>");
                            }
                            break;

                        default:
                            leitor.printAbove("Comando desconhecido");
                    }
                } catch (Exception e) {
                    leitor.printAbove("[Erro ao executar comando no RabbitMQ: " + e.getMessage() + "]");
                }

                continue;
            }

            if (entrada.equalsIgnoreCase("/sair")) {
                leitor.printAbove("Encerrando o chat...");
                break;
            }

            if (entrada.startsWith("@") || entrada.startsWith("#")) {
                String nomeDestino = entrada.substring(1).trim();

                if (nomeDestino.isEmpty()) {
                    leitor.printAbove("[Erro: informe um nome após o símbolo]");
                    continue;
                }

                destinatarioAtual = entrada;
                continue;
            }

            if (destinatarioAtual.isEmpty()) {
                leitor.printAbove("[Erro: Defina um destinatário primeiro. Ex: @joao ou #grupo]");
                continue;
            }

            try {
                if (destinatarioAtual.startsWith("#")) {
                    String nomeGrupo = destinatarioAtual.substring(1);
                    service.enviarMensagemGrupo(nomeGrupo, entrada);
                } else {
                    String nomeUsuario = destinatarioAtual.substring(1);
                    service.enviarMensagem(nomeUsuario, entrada);
                }
            } catch (Exception e) {
                leitor.printAbove("[Erro ao enviar mensagem: " + e.getMessage() + "]");
            }
        }
    }
}