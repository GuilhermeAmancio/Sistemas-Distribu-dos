package br.ufs.dcomp.ChatRabbitMQ;

import java.util.Scanner;

public class Main{
    public static void main(String[] args){
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite seu usuario: ");
        String usuario = scanner.nextLine().trim();

        RabbitMQService service = new RabbitMQService(
            "98.92.33.130",
            "admin",
            "password",
            usuario
        );

        try {
            service.conectar();
            service.criarFilaUsuario();

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