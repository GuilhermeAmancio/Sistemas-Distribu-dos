package br.ufs.dcomp.ChatRabbitMQ;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

public class RabbitMQService {

    private static final String TIPO_PROTOBUF = "application/x-protobuf";

    private static final Path PASTA_RECEBIDOS =
            Paths.get("arquivos_recebidos")
                    .toAbsolutePath()
                    .normalize();

    private final String host;
    private final String username;
    private final String password;
    private final String usernameUsuario;

    private Connection connection;
    private Channel channel;
    private Channel receiveChannel;

    public RabbitMQService(
            String host,
            String username,
            String password,
            String usernameUsuario) {

        this.host = host;
        this.username = username;
        this.password = password;
        this.usernameUsuario = usernameUsuario;
    }

    /**
     * Estabelece a conexão com o RabbitMQ e cria os canais
     * de envio e recebimento.
     */
    public void conectar() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(5672);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost("/");
        factory.setConnectionTimeout(10000);

        connection = factory.newConnection();
        channel = connection.createChannel();
        receiveChannel = connection.createChannel();

        System.out.println("Conectado ao RabbitMQ.");
    }

    /**
     * Cria a fila do usuário.
     */
    public void criarFilaUsuario() throws IOException {
        verificarConexao();
        channel.queueDeclare(
                usernameUsuario,
                true,
                false,
                false,
                null
        );
        System.out.println("Fila criada: " + usernameUsuario);
    }

    /**
     * Envia uma mensagem de texto para a fila do destinatário.
     */
    public void enviarMensagem(
            String destinatario,
            String texto) throws IOException {

        verificarConexao();

        String mensagem = MensagemTextoFormatada.criar(
                usernameUsuario,
                destinatario,
                texto
        );

        AMQP.BasicProperties propriedades =
                new AMQP.BasicProperties.Builder()
                        .contentType("text/plain")
                        .contentEncoding("UTF-8")
                        .deliveryMode(2)
                        .build();

        channel.basicPublish(
                "",
                destinatario,
                propriedades,
                mensagem.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * Envia uma mensagem de texto para um grupo.
     */
    public void enviarMensagemGrupo(
            String nomeGrupo,
            String texto) throws IOException {

        verificarConexao();

        byte[] payload = MensagemCodec.serializarTexto(
                usernameUsuario,
                nomeGrupo,
                texto
        );

        AMQP.BasicProperties propriedades =
                new AMQP.BasicProperties.Builder()
                        .contentType("text/plain")
                        .contentEncoding("UTF-8")
                        .deliveryMode(2)
                        .build();

        channel.basicPublish(
                nomeGrupo,
                "",
                propriedades,
                payload
        );
    }

    /**
     * Publica uma mensagem Protobuf contendo um arquivo.
     *
     * Os dados devem conter a mensagem serializada pelo MensagemCodec.
     */
    public void enviarArquivo(
            String destinatario,
            byte[] dados) throws IOException {

        verificarConexao();

        if (dados == null || dados.length == 0) {
            throw new IOException(
                    "Não é possível enviar um arquivo vazio ou sem dados."
            );
        }

        AMQP.BasicProperties propriedades =
                new AMQP.BasicProperties.Builder()
                        .contentType(TIPO_PROTOBUF)
                        .deliveryMode(2)
                        .build();

        channel.basicPublish(
                "",
                destinatario,
                propriedades,
                dados
        );
    }

    /**
     * Publica uma mensagem Protobuf contendo um arquivo para um grupo.
     * Utiliza o exchange do tipo fanout correspondente ao grupo.
     */
    public void enviarArquivoGrupo(
            String nomeGrupo,
            byte[] dados) throws IOException {

        verificarConexao();

        if (dados == null || dados.length == 0) {
            throw new IOException(
                    "Não é possível enviar um arquivo vazio ou sem dados."
            );
        }

        AMQP.BasicProperties propriedades =
                new AMQP.BasicProperties.Builder()
                        .contentType(TIPO_PROTOBUF)
                        .deliveryMode(2)
                        .build();

        channel.basicPublish(
                nomeGrupo,
                "",
                propriedades,
                dados
        );
    }

    public void criarExchangeGrupo(String nomeGrupo) throws IOException {
        verificarConexao();
        channel.exchangeDeclare(nomeGrupo, "fanout", true);
    }

    public void adicionarUsuarioAoExchange(String usuario, String nomeGrupo) throws IOException {
        verificarConexao();
        channel.queueBind(usuario, nomeGrupo, "");
    }

    public void removerUsuarioDoExchange(String usuario, String nomeGrupo) throws IOException {
        verificarConexao();
        channel.queueUnbind(usuario, nomeGrupo, "");
    }

    public void removerExchangeGrupo(String nomeGrupo) throws IOException {
        verificarConexao();
        channel.exchangeDelete(nomeGrupo);
    }

    /**
     * Registra o consumidor da fila do usuário.
     *
     * Mensagens Protobuf são interpretadas como arquivos.
     * Mensagens textuais continuam sendo entregues ao consumidor
     * no formato original.
     */
    public void receberMensagens(
            Consumer<String> consumidor) throws IOException {

        verificarConexao();

        com.rabbitmq.client.Consumer rabbitConsumer =
                new DefaultConsumer(receiveChannel) {

                    @Override
                    public void handleDelivery(
                            String consumerTag,
                            Envelope envelope,
                            AMQP.BasicProperties properties,
                            byte[] body) throws IOException {

                        String tipoConteudo = properties.getContentType();

                        if (TIPO_PROTOBUF.equals(tipoConteudo)) {
                            processarMensagemProtobuf(body, consumidor);
                        } else {
                            String mensagem = new String(
                                    body,
                                    StandardCharsets.UTF_8
                            );
                            consumidor.accept(mensagem);
                        }
                    }
                };

        receiveChannel.basicConsume(
                usernameUsuario,
                true,
                rabbitConsumer
        );
    }

    /**
     * Desserializa a mensagem Protobuf, extrai os metadados
     * e salva o conteúdo do arquivo recebido.
     */
    private void processarMensagemProtobuf(
            byte[] dados,
            Consumer<String> consumidor) {

        try {
            ChatProtocol.Mensagem mensagem =
                    MensagemCodec.desserializar(dados);

            if (!mensagem.hasConteudo()) {
                consumidor.accept(
                        "Erro: a mensagem recebida não possui conteúdo."
                );
                return;
            }

            ChatProtocol.Conteudo conteudo = mensagem.getConteudo();

            String nomeArquivo = conteudo.getNome();
            String tipoMime = conteudo.getTipo();

            if (nomeArquivo == null || nomeArquivo.isBlank()) {
                if (conteudo.hasCorpo() && (tipoMime == null || tipoMime.equals("text/plain"))) {
                    String texto = conteudo.getCorpo().toString(StandardCharsets.UTF_8);
                    String dataHora = mensagem.getData() + " às " + mensagem.getHora();
                    String grupo = mensagem.getGrupo();
                    String emissor = mensagem.getEmissor();

                    if (!grupo.isEmpty()) {
                        consumidor.accept("(" + dataHora + ") #" + grupo + " <=> @" + emissor + " diz: " + texto);
                    } else {
                        consumidor.accept("(" + dataHora + ") @" + emissor + " diz: " + texto);
                    }
                    return;
                }
            }

            /*
             * Obtém somente o nome final do arquivo.
             * Isso impede que o remetente escolha outro diretório
             * por meio do nome enviado.
             */
            Path nomeSeguro = Paths.get(nomeArquivo).getFileName();

            if (nomeSeguro == null
                    || nomeSeguro.toString().isBlank()
                    || nomeSeguro.toString().equals(".")
                    || nomeSeguro.toString().equals("..")) {

                consumidor.accept("Erro: nome de arquivo inválido.");
                return;
            }

            /*
             * Cria a pasta de recebimento se ela ainda não existir.
             */
            Files.createDirectories(PASTA_RECEBIDOS);

            Path destino = PASTA_RECEBIDOS
                    .resolve(nomeSeguro.toString())
                    .normalize();

            /*
             * Confirma que o destino permanece dentro da pasta
             * destinada aos arquivos recebidos.
             */
            if (!destino.startsWith(PASTA_RECEBIDOS)) {
                consumidor.accept("Erro: caminho de destino inválido.");
                return;
            }

            /*
             * Salva os bytes originais do arquivo.
             * Se já existir um arquivo com o mesmo nome, ele será
             * substituído.
             */
            Files.write(
                    destino,
                    conteudo.getCorpo().toByteArray()
            );

            String dataHora = mensagem.getData() + " às " + mensagem.getHora();
            String remetente = mensagem.getEmissor();
            String grupo = mensagem.getGrupo();

            if (!grupo.isEmpty()) {
                consumidor.accept("(" + dataHora + ") Arquivo \"" + nomeSeguro + "\" recebido do grupo #" + grupo + " por @" + remetente + " !");
            } else {
                consumidor.accept("(" + dataHora + ") Arquivo \"" + nomeSeguro + "\" recebido de @" + remetente + " !");
            }
            consumidor.accept("[Arquivo salvo em] " + destino);

        } catch (InvalidProtocolBufferException e) {
            System.err.println("Erro ao interpretar mensagem Protobuf: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Erro ao salvar o arquivo recebido: " + e.getMessage());
        }
    }

    /**
     * Retorna o nome do usuário conectado.
     */
    public String getUsernameUsuario() {
        return usernameUsuario;
    }

    /**
     * Verifica se a conexão e o canal de envio estão disponíveis.
     */
    private void verificarConexao() throws IOException {
        if (connection == null || !connection.isOpen()) {
            throw new IOException("Não existe uma conexão ativa com o RabbitMQ.");
        }
        if (channel == null || !channel.isOpen()) {
            throw new IOException("O canal de envio do RabbitMQ não está disponível.");
        }
        if (receiveChannel == null || !receiveChannel.isOpen()) {
            throw new IOException("O canal de recebimento do RabbitMQ não está disponível.");
        }
    }

    /**
     * Fecha os canais e a conexão com o RabbitMQ.
     */
    public void fechar() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (receiveChannel != null && receiveChannel.isOpen()) {
                receiveChannel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}