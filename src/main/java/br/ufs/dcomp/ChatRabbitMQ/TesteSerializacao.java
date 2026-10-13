package br.ufs.dcomp.ChatRabbitMQ;

public class TesteSerializacao {

    public static void main(String[] args) throws Exception {

        String textoOriginal = "Ola, pessoal! Teste do Protobuf.";

        // Serializa a mensagem para bytes
        byte[] dados = MensagemCodec.serializarTexto(
                "Maria",
                "turma",
                textoOriginal
        );

        System.out.println("Mensagem serializada!");
        System.out.println("Tamanho em bytes: " + dados.length);

        // Recupera a mensagem a partir dos bytes
        ChatProtocol.Mensagem mensagem =
                MensagemCodec.desserializar(dados);

        // Exibe os campos recuperados
        System.out.println("\nMensagem recuperada:");
        System.out.println("Emissor: " + mensagem.getEmissor());
        System.out.println("Data: " + mensagem.getData());
        System.out.println("Hora: " + mensagem.getHora());
        System.out.println("Grupo: " + mensagem.getGrupo());
        System.out.println("Tipo: " + mensagem.getConteudo().getTipo());
        System.out.println("Conteudo: " + MensagemCodec.obterTexto(mensagem));

        // Confere se os dados continuam iguais
        boolean testePassou =
                mensagem.getEmissor().equals("Maria")
                && mensagem.getGrupo().equals("turma")
                && mensagem.getConteudo().getTipo().equals("text/plain")
                && MensagemCodec.obterTexto(mensagem).equals(textoOriginal);

        if (testePassou) {
            System.out.println("\nSUCESSO: teste passou!");
        } else {
            throw new IllegalStateException(
                    "FALHA: os dados recuperados sao diferentes dos originais."
            );
        }
    }
}
