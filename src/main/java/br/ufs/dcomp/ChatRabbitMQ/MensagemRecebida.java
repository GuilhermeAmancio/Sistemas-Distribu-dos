package br.ufs.dcomp.ChatRabbitMQ;

import org.jline.reader.LineReader;

public class MensagemRecebida implements Runnable {

    private final RabbitMQService service;
    private final LineReader leitor;

    public MensagemRecebida(
            RabbitMQService service,
            LineReader leitor) {

        this.service = service;
        this.leitor = leitor;
    }

    @Override
    public void run() {
        try {
            service.receberMensagens(mensagem -> {

                // Mensagens de arquivo: exibe a notificação
                // sem tentar interpretá-las como mensagens de texto.
                if (mensagem.startsWith("[Arquivo recebido] ")
                        || mensagem.startsWith("[Arquivo salvo em] ")) {
                    leitor.printAbove(mensagem);
                    return;
                }

                // Mensagens de texto no formato antigo:
                // remetente|data/hora|destinatário|texto
                String[] partes = mensagem.split("\\|", 4);

                if (partes.length < 4) {
                    leitor.printAbove(
                            "Mensagem recebida em formato não reconhecido: "
                                    + mensagem);
                    return;
                }

                Mensagem message = new Mensagem(
                        partes[0],
                        partes[1],
                        partes[2],
                        partes[3]
                );

                String mensagemFormatada =
                        "(" + message.getDataHora() + ") @"
                                + message.getRemetente()
                                + " diz: "
                                + message.getTexto();

                leitor.printAbove(mensagemFormatada);
            });

        } catch (Exception e) {
            leitor.printAbove(
                    "Erro ao receber mensagem: " + e.getMessage()
            );
        }
    }
}