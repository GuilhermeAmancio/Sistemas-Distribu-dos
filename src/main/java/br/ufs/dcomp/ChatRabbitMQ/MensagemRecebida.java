package br.ufs.dcomp.ChatRabbitMQ;

import org.jline.reader.LineReader;

public class MensagemRecebida implements Runnable {
    private final RabbitMQService service;
    private final LineReader leitor;

    public MensagemRecebida(RabbitMQService service, LineReader leitor) {
        this.service = service;
        this.leitor = leitor;
    }

    @Override
    public void run() {
        try {
            // O serviço agora consome o objeto de mensagem do Protobuf
            service.receberMensagens(mensagem -> {
                String emissor = mensagem.getEmissor();
                String data = mensagem.getData();
                String hora = mensagem.getHora();
                String grupo = mensagem.getGrupo();
                String texto = MensagemCodec.obterTexto(mensagem);

                String mensagemFormatada;

                // Verifica se a mensagem veio de um grupo ou individualmente
                if (grupo == null || grupo.isEmpty()) {
                    mensagemFormatada = "(" + data + " às " + hora + ") @" + emissor + " diz: " + texto;
                } else {
                    mensagemFormatada = "(" + data + " às " + hora + ") " + emissor + "#" + grupo + " diz: " + texto;
                }

                leitor.printAbove(mensagemFormatada);
            });
        } catch (Exception e) {
            leitor.printAbove("Erro ao receber mensagem: " + e.getMessage());
        }
    }
}