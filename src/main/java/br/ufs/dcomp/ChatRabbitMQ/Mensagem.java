package br.ufs.dcomp.ChatRabbitMQ;

public class Mensagem{
    private String remetente;
    private String destinatario;
    private String texto;
    private String dataHora;

    public Mensagem(String remetente, String destinatario, String texto, String dataHora){
        this.remetente = remetente;
        this.destinatario = destinatario;
        this.texto = texto;
        this.dataHora = dataHora;
    }

    public String getRemetente(){
        return remetente;
    }

    public String getDestinatario(){
        return destinatario;
    }

    public String getTexto(){
        return texto;
    }

    public String getDataHora(){
        return dataHora;
    }


}