package br.ufs.dcomp.ChatRabbitMQ;

import org.jline.reader.LineReader;


public class MensagemRecebida implements Runnable{
    private final RabbitMQService service;
    private final LineReader leitor;

    public MensagemRecebida(RabbitMQService service, LineReader leitor){
        this.service = service;
        this.leitor = leitor;
    }

    @Override
    public void run(){
        try{
            service.receberMensagens(mensagem ->{
                String[] partes = mensagem.split("\\|"); //comando para separar a mensagem do formato que ela chega com |
                if (partes.length < 4){
                    leitor.printAbove("Erro: Formato invalido da mensagem");
                    return;
                }

                Mensagem message = new Mensagem(partes[0], partes[1], partes[2], partes[3]); //estabelece a mensagem em quatro campos
                
                //formata a mensagem no formato pedido
                String mensagemFormatada = "(" + message.getDataHora() + ") @" + message.getRemetente() + " diz: " + message.getTexto();
                //printabove em todas as classes deste codigo ajuda a imprimir mensagens e erros sem quebrar o que esta sendo
                //digitado pelo usuario
                leitor.printAbove(mensagemFormatada);
            });
        } catch (Exception e){
            leitor.printAbove(
                "Erro ao receber mensagem: " + e.getMessage()
            );
        }
    }
}