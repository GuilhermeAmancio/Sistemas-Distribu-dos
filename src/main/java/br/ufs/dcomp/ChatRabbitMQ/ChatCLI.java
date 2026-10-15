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
                    : destinatarioAtual.startsWith("#") 
                        ? destinatarioAtual + "<< " 
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

                try {
                    switch (partes[0]) {
                        case "!addGroup":
                            if (partes.length == 2) {
                                String nomeGrupo = partes[1];
                                String resposta = groupService.criarGrupo(nomeGrupo, service.getUsernameUsuario());
                                // Integração com o RabbitMQ: Cria o exchange fanout e vincula o criador
                                service.criarExchangeGrupo(nomeGrupo);
                                service.adicionarUsuarioAoExchange(service.getUsernameUsuario(), nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Metodo de uso: !addGroup <grupo>");
                            }
                            break;

                        case "!addUser":
                            if (partes.length == 3) {
                                String usuarioAlvo = partes[1];
                                String nomeGrupo = partes[2];
                                String resposta = groupService.adicionarUsuario(usuarioAlvo, nomeGrupo);
                                // Integração com o RabbitMQ: Vincula a fila do usuário ao exchange do grupo
                                service.adicionarUsuarioAoExchange(usuarioAlvo, nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Metodo de uso: !addUser <usuario> <grupo>");
                            }
                            break;

                        case "!removeUser":
                            if (partes.length == 3) {
                                String usuarioAlvo = partes[1];
                                String nomeGrupo = partes[2];
                                String resposta = groupService.removerUsuario(usuarioAlvo, nomeGrupo);
                                // Integração com o RabbitMQ: Desvincula a fila do usuário do exchange
                                service.removerUsuarioDoExchange(usuarioAlvo, nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Metodo de uso: !removeUser <usuario> <grupo>");
                            }
                            break;

                        case "!removeGroup":
                            if (partes.length == 2) {
                                String nomeGrupo = partes[1];
                                String resposta = groupService.removerGrupo(nomeGrupo);
                                // Integração com o RabbitMQ: Exclui o exchange do grupo
                                service.removerExchangeGrupo(nomeGrupo);
                                leitor.printAbove(resposta);
                            } else {
                                leitor.printAbove("Metodo de uso: !removeGroup <grupo>");
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

            // Tratamento para seleção de destino (Usuário com @ ou Grupo com #)
            if (entrada.startsWith("@") || entrada.startsWith("#")) {
                String novoDestinatario = entrada.substring(1).trim();

                if (novoDestinatario.isEmpty()) {
                    leitor.printAbove("[Erro: informe um nome após o símbolo]");
                    continue;
                }

                destinatarioAtual = entrada; // Mantém o '@' ou '#' para sabermos o tipo depois
                continue;
            }

            if (destinatarioAtual.isEmpty()) {
                leitor.printAbove("[Erro: Defina um destinatário primeiro. Ex: @joao ou #grupo]");
                continue;
            }

            try {
                // Verifica se o destino atual é um grupo ou um usuário 
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