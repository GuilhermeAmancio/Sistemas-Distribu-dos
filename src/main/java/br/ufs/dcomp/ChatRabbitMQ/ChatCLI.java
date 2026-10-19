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
                    : "@" + destinatarioAtual + "<< ";

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
            if (entrada.startsWith("@")) {

                String novoDestinatario =
                        entrada.substring(1).trim();

                if (novoDestinatario.isEmpty()) {

                    leitor.printAbove(
                            "[Erro: informe um usuário após @]"
                    );

                    continue;
                }

                destinatarioAtual = novoDestinatario;

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
                                + "Exemplo: @joao]"
                );

                continue;
            }

            /*
             * Envio de mensagens textuais.
             */
            try {

                service.enviarMensagem(
                        destinatarioAtual,
                        entrada
                );

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

        if (entrada.startsWith("!upload")) {

            tratarUpload(entrada);

        } else {

            leitor.printAbove(
                    "[Comando desconhecido: " + entrada + "]"
            );
        }
    }

    /**
     * Valida o comando !upload e inicia o envio em segundo plano.
     */
    private void tratarUpload(String entrada) {

        if (destinatarioAtual.isEmpty()) {

            leitor.printAbove(
                    "[Erro: selecione um destinatário antes de enviar "
                            + "um arquivo. Exemplo: @marcio]"
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
                        + "\" para @"
                        + destinatarioFinal
                        + "."
        );

        /*
         * A leitura e a publicação ocorrem em segundo plano.
         */
        executorUploads.submit(() -> {

            try {

                byte[] dados = ArquivoService.prepararArquivo(
                        service.getUsernameUsuario(),
                        "",
                        caminho.toString()
                );

                service.enviarArquivo(
                        destinatarioFinal,
                        dados
                );

                leitor.printAbove(
                        "Arquivo \""
                                + nomeArquivo
                                + "\" foi enviado para @"
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