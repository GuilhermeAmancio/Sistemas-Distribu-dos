package br.ufs.dcomp.ChatRabbitMQ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

public final class ArquivoService {

    private ArquivoService() {
    }

    public static byte[] prepararArquivo(
            String emissor,
            String grupo,
            String caminhoArquivo) throws IOException {

        // Converte o caminho informado para um caminho absoluto.
        Path caminho = Paths.get(caminhoArquivo)
                .toAbsolutePath()
                .normalize();

        // Verifica se o arquivo existe.
        if (!Files.exists(caminho)) {
            throw new IOException(
                    "O arquivo informado não existe."
            );
        }

        // Verifica se o caminho aponta para um arquivo.
        if (!Files.isRegularFile(caminho)) {
            throw new IOException(
                    "O caminho informado não é um arquivo."
            );
        }

        // Verifica se o arquivo pode ser lido.
        if (!Files.isReadable(caminho)) {
            throw new IOException(
                    "O arquivo não possui permissão de leitura."
            );
        }

        // Identifica o tipo MIME do arquivo.
        String tipoMime = Files.probeContentType(caminho);

        if (tipoMime == null || tipoMime.isBlank()) {
            tipoMime = "application/octet-stream";
        }

        // Lê o conteúdo binário do arquivo.
        byte[] corpo = Files.readAllBytes(caminho);

        // Obtém somente o nome do arquivo.
        String nomeArquivo = caminho.getFileName().toString();

        // Prepara a mensagem no formato Protobuf.
        return MensagemCodec.serializar(
                emissor,
                LocalDateTime.now(),
                grupo,
                tipoMime,
                corpo,
                nomeArquivo
        );
    }
}