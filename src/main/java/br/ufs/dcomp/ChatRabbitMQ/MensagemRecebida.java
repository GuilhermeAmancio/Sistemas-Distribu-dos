package br.ufs.dcomp.ChatRabbitMQ;


public class MensagemRecebida implements Runnable{
    private final RabbitMQService service;

    public MensagemRecebida(RabbitMQService service){
        this.service = service;
    }

    @Override
    public void run(){
        try{
            service.receberMensagens(mensagem ->{
                String[] partes = mensagem.split("\\|");
                if (partes.length < 4){
                    System.err.println("Erro: Formato invalido da mensagem");
                    return;
                }

                Mensagem message = new Mensagem(partes[0], partes[1], partes[2], partes[3]);
                
                System.out.println("(" + message.getDataHora() + ") @" + message.getRemetente() + " diz: " + message.getTexto());
            });
        } catch (Exception e){
            System.err.println(
                "Erro ao receber mensagem: " + e.getMessage()
            );
        }
    }
}