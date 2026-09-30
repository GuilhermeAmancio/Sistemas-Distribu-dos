package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

public class RabbitMQService {

    private final String host;
    private final String username;
    private final String password;

    private Connection connection;
    private Channel channel;
    private Channel receiveChannel;

    private String usernameUsuario;

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

    public void conectar() throws Exception {

        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost("/");

        connection = factory.newConnection();
        channel = connection.createChannel();
        receiveChannel = connection.createChannel();

        System.out.println("Conectado ao RabbitMQ.");
    }

    public void criarFilaUsuario() throws IOException {

        channel.queueDeclare(
                usernameUsuario,
                true,
                false,
                false,
                null
        );

        System.out.println(
                "Fila criada: " + usernameUsuario
        );
    }

    public void enviarMensagem(
            String destinatario,
            String texto) throws IOException {

        LocalDateTime agora = LocalDateTime.now();
        DateTimeFormatter formato = DateTimeFormatter.ofPattern("dd/MM/yyyy 'às' HH:mm");
        String dataHora = agora.format(formato);
        String mensagem = usernameUsuario + "|" + destinatario + "|" + texto + "|" + dataHora; 

        channel.basicPublish(
                "",
                destinatario,
                null,
                mensagem.getBytes(StandardCharsets.UTF_8)
        );
    }

    public void receberMensagens(
            Consumer<String> consumidor) throws IOException {

        com.rabbitmq.client.Consumer rabbitConsumer =
                new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(
                    String consumerTag,
                    Envelope envelope,
                    AMQP.BasicProperties properties,
                    byte[] body)
                    throws IOException {

                String mensagem =
                        new String(
                                body,
                                StandardCharsets.UTF_8
                        );

                consumidor.accept(mensagem);
            }
        };

        receiveChannel.basicConsume(
                usernameUsuario,
                true,
                rabbitConsumer
        );
    }

    public String getUsernameUsuario() {
        return usernameUsuario;
    }

    public void fechar() {

        try {

            if (channel != null && channel.isOpen()) {
                channel.close();
            }

            if (receiveChannel != null && receiveChannel.isOpen()){
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
