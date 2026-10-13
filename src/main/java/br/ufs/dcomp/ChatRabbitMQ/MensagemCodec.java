
package br.ufs.dcomp.ChatRabbitMQ;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class MensagemCodec {

    private static final DateTimeFormatter FORMATO_DATA =
            DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private static final DateTimeFormatter FORMATO_HORA =
            DateTimeFormatter.ofPattern("HH:mm:ss");

    private MensagemCodec() {
    }

    public static byte[] serializar(
            String emissor,
            LocalDateTime instante,
            String grupo,
            String tipo,
            byte[] corpo,
            String nome) {

        ChatProtocol.Conteudo conteudo =
                ChatProtocol.Conteudo.newBuilder()
                        .setTipo(tipo)
                        .setCorpo(ByteString.copyFrom(corpo))
                        .setNome(nome == null ? "" : nome)
                        .build();

        ChatProtocol.Mensagem mensagem =
                ChatProtocol.Mensagem.newBuilder()
                        .setEmissor(emissor)
                        .setData(instante.format(FORMATO_DATA))
                        .setHora(instante.format(FORMATO_HORA))
                        .setGrupo(grupo == null ? "" : grupo)
                        .setConteudo(conteudo)
                        .build();

        return mensagem.toByteArray();
    }

    public static byte[] serializarTexto(
            String emissor,
            String grupo,
            String texto) {

        return serializar(
                emissor,
                LocalDateTime.now(),
                grupo,
                "text/plain",
                texto.getBytes(StandardCharsets.UTF_8),
                ""
        );
    }

    public static ChatProtocol.Mensagem desserializar(byte[] dados)
            throws InvalidProtocolBufferException {

        return ChatProtocol.Mensagem.parseFrom(dados);
    }

    public static String obterTexto(ChatProtocol.Mensagem mensagem) {
        return mensagem.getConteudo()
                .getCorpo()
                .toString(StandardCharsets.UTF_8);
    }
}
