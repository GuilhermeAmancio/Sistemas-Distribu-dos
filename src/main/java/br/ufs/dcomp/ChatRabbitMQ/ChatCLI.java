package br.ufs.dcomp.ChatRabbitMQ;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatCLI {

    private final RabbitMQService service;

    private String destinatarioAtual = "";

    private LineReader leitor;

    private final GroupService groupService = new GroupService();

    /*
     * Executor responsável por executar uploads em segundo plano.
     */
    private final ExecutorService executorUploads =
            Executors.newSingleThreadExecutor();

    public ChatCLI(RabbitMQService service) {
        this.service = service;
    }

    public void iniciar() {

        leitor = LineReaderBuilder.builder().build();

        Thread threadReceba = new Thread(
                new MensagemRecebida(service, leitor)
        );

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

                System.err.println(
                        "[Erro ao ler entrada: "
                                + e.getMessage()
                                + "]"
                );

                break;
            }

            entrada = entrada.trim();

            if (entrada.isEmpty()) {
                continue;
            }

            /*
             * Comando para encerrar o chat.
             */
            if (entrada.equalsIgnoreCase("/sair")) {

                leitor.printAbove("Encerrando o chat...");

                break;
            }

            /*
             * Comando para selecionar um destinatário.
             */
            if (entrada.startsWith("@") || entrada.startsWith("#")) {

                String nomeDestino =
                        entrada.substring(1).trim();

                if (nomeDestino.isEmpty()) {

                    leitor.printAbove(
                            "[Erro: informe um nome após o símbolo]"
                    );

                    continue;
                }

                destinatarioAtual = entrada;

                continue;
            }

            /*
             * Comandos iniciados com !.
             */
            if (entrada.startsWith("!")) {

                tratarComando(entrada);

                continue;
            }

            /*
             * Para enviar uma mensagem, é necessário selecionar
             * primeiro um destinatário.
             */
            if (destinatarioAtual.isEmpty()) {

                leitor.printAbove(
                        "[Erro: defina um destinatário primeiro. "
                                + "Exemplo: @joao ou #grupo]"
                );

                continue;
            }

            /*
             * Envio de mensagens textuais.
             */
            try {
                if (destinatarioAtual.startsWith("#")) {
                    String nomeGrupo = destinatarioAtual.substring(1);
                    service.enviarMensagemGrupo(nomeGrupo, entrada);
                } else {
                    String nomeUsuario = destinatarioAtual.substring(1);
                    service.enviarMensagem(nomeUsuario, entrada);
                }
            } catch (Exception e) {

                leitor.printAbove(
                        "[Erro ao enviar mensagem: "
                                + e.getMessage()
                                + "]"
                );
            }
        }

        /*
         * Não aceita novos uploads depois do encerramento.
         */
        executorUploads.shutdown();
    }

    /**
     * Trata os comandos da interface.
     */
    private void tratarComando(String entrada) {
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

                case "!upload":
                    tratarUpload(entrada);
                    break;

                default:
                    leitor.printAbove("Comando desconhecido");
            }
        } catch (Exception e) {
            leitor.printAbove("[Erro ao executar comando no RabbitMQ: " + e.getMessage() + "]");
        }
    }

    /**
     * Valida o comando !upload e inicia o envio em segundo plano.
     */
    private void tratarUpload(String entrada) {

        if (destinatarioAtual.isEmpty()) {

            leitor.printAbove(
                    "[Erro: selecione um destinatário antes de enviar "
                            + "um arquivo. Exemplo: @marcio ou #grupo]"
            );

            return;
        }

        /*
         * Obtém o caminho informado depois do comando.
         *
         * Aceita caminhos entre aspas, inclusive com espaços.
         */
        String caminhoInformado =
                entrada.substring("!upload".length()).trim();

        if (caminhoInformado.isEmpty()) {

            leitor.printAbove(
                    "Uso: !upload <caminho-do-arquivo>"
            );

            return;
        }

        if (caminhoInformado.startsWith("\"")
                && caminhoInformado.endsWith("\"")
                && caminhoInformado.length() >= 2) {

            caminhoInformado = caminhoInformado.substring(
                    1,
                    caminhoInformado.length() - 1
            );
        }

        final String caminhoFinal = caminhoInformado;

        /*
         * Captura o destinatário atual para que mudanças no prompt
         * não alterem o destino de um upload já iniciado.
         */
        final String destinatarioFinal = destinatarioAtual;

        final Path caminho;

        try {

            caminho = Paths.get(caminhoFinal)
                    .toAbsolutePath()
                    .normalize();

        } catch (Exception e) {

            leitor.printAbove(
                    "[Erro: caminho de arquivo inválido.]"
            );

            return;
        }

        /*
         * Validação inicial do arquivo.
         */
        try {

            if (!java.nio.file.Files.exists(caminho)) {

                leitor.printAbove(
                        "[Erro: o arquivo informado não existe.]"
                );

                return;
            }

            if (!java.nio.file.Files.isRegularFile(caminho)) {

                leitor.printAbove(
                        "[Erro: o caminho informado não é um arquivo.]"
                );

                return;
            }

            if (!java.nio.file.Files.isReadable(caminho)) {

                leitor.printAbove(
                        "[Erro: o arquivo não pode ser lido.]"
                );

                return;
            }

        } catch (Exception e) {

            leitor.printAbove(
                    "[Erro ao validar arquivo: "
                            + e.getMessage()
                            + "]"
            );

            return;
        }

        String nomeArquivo = caminho.getFileName().toString();

        leitor.printAbove(
                "Enviando \""
                        + nomeArquivo
                        + "\" para "
                        + destinatarioFinal
                        + "."
        );

        /*
         * A leitura e a publicação ocorrem em segundo plano.
         */
        executorUploads.submit(() -> {

            try {
                String grupoParam = destinatarioFinal.startsWith("#") ? destinatarioFinal.substring(1) : "";

                byte[] dados = ArquivoService.prepararArquivo(
                        service.getUsernameUsuario(),
                        grupoParam,
                        caminho.toString()
                );

                if (destinatarioFinal.startsWith("#")) {
                    String nomeGrupo = destinatarioFinal.substring(1);
                    service.enviarArquivoGrupo(nomeGrupo, dados);
                } else {
                    String nomeUsuario = destinatarioFinal.startsWith("@")
                            ? destinatarioFinal.substring(1)
                            : destinatarioFinal;
                    service.enviarArquivo(nomeUsuario, dados);
                }

                leitor.printAbove(
                        "Arquivo \""
                                + nomeArquivo
                                + "\" foi enviado para "
                                + destinatarioFinal
                                + " !"
                );

            } catch (IOException e) {

                leitor.printAbove(
                        "[Erro ao enviar arquivo \""
                                + nomeArquivo
                                + "\": "
                                + e.getMessage()
                                + "]"
                );

            } catch (Exception e) {

                leitor.printAbove(
                        "[Erro inesperado no envio do arquivo: "
                                + e.getMessage()
                                + "]"
                );
            }
        });
    }
}