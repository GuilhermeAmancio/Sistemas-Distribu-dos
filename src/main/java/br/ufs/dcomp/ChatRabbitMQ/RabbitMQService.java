package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
        System.out.println("Fila criada: " + usernameUsuario);
    }

    // Envio de mensagem individual (Usando Protobuf) 
    public void enviarMensagem(
            String destinatario,
            String texto) throws IOException {

        // Serializa usando o codec do Protobuf (grupo vazio)
        byte[] payload = MensagemCodec.serializarTexto(
                usernameUsuario,
                "",
                texto
        );

        channel.basicPublish(
                "", // Exchange default para filas diretas
                destinatario,
                null,
                payload
        );
    }

    // Envio de mensagem para grupo (Usando Exchange fanout)
    public void enviarMensagemGrupo(
            String nomeGrupo,
            String texto) throws IOException {

        
        byte[] payload = MensagemCodec.serializarTexto(
                usernameUsuario,
                nomeGrupo,
                texto
        );

    
        channel.basicPublish(
                nomeGrupo,
                "",
                null,
                payload
        );
    }

    // Gestão de grupos no RabbitMQ (Exchanges e Bindings) 

    public void criarExchangeGrupo(String nomeGrupo) throws IOException {
        // Declara um Exchange fanout para o grupo
        channel.exchangeDeclare(nomeGrupo, "fanout", true);
    }

    public void adicionarUsuarioAoExchange(String usuario, String nomeGrupo) throws IOException {
        // Cria o vínculo (binding) da fila do usuário ao Exchange do grupo
        channel.queueBind(usuario, nomeGrupo, "");
    }

    public void removerUsuarioDoExchange(String usuario, String nomeGrupo) throws IOException {
        // Remove o vínculo da fila do usuário com o Exchange do grupo
        channel.queueUnbind(usuario, nomeGrupo, "");
    }

    public void removerExchangeGrupo(String nomeGrupo) throws IOException {
        // Exclui o Exchange do grupo
        channel.exchangeDelete(nomeGrupo);
    }

    // Recebimento de mensagens
    public void receberMensagens(
            Consumer<ChatProtocol.Mensagem> consumidor) throws IOException {

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                ChatProtocol.Mensagem mensagem = MensagemCodec.desserializar(delivery.getBody());
                consumidor.accept(mensagem);
            } catch (Exception e) {
                System.err.println("Erro ao desserializar mensagem: " + e.getMessage());
            }
        };

        receiveChannel.basicConsume(
                usernameUsuario,
                true,
                deliverCallback,
                consumerTag -> {}
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