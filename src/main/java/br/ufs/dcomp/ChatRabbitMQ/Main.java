package br.ufs.dcomp.ChatRabbitMQ;

import java.util.Scanner;

public class Main{
    public static void main(String[] args){
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite seu usuario: "); //estabelecimento do usuario que esta usando o serviço
        String usuario = scanner.nextLine().trim();

        //estabelecimento da conexao com o rabbit, é necessário alterar o ip para o ip atual da instância
        RabbitMQService service = new RabbitMQService(
            "34.237.51.175",
            "admin",
            "password",
            usuario
        );

        try {
            service.conectar();
            service.criarFilaUsuario(); //cria a fila com o nome do usuario no rabbbit

            ChatCLI cli = new ChatCLI(service);
            cli.iniciar();
        } catch (Exception e){
            System.err.println("Erro: " + e.getMessage());
        } finally{
            service.fechar();
            scanner.close();
        }
    }
}