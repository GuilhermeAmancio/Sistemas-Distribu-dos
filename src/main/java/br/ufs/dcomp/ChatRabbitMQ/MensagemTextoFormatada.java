package br.ufs.dcomp.ChatRabbitMQ;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class MensagemTextoFormatada {

    private static final DateTimeFormatter FORMATO =
            DateTimeFormatter.ofPattern("dd/MM/yyyy 'às' HH:mm");

    private MensagemTextoFormatada() {
    }

    public static String criar(
            String emissor,
            String destinatario,
            String texto) {

        String dataHora = LocalDateTime.now().format(FORMATO);

        return emissor
                + "|" + destinatario
                + "|" + texto
                + "|" + dataHora;
    }
}