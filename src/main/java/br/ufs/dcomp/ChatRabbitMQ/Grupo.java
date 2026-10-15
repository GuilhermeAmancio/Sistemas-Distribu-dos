package br.ufs.dcomp.ChatRabbitMQ;

import java.util.HashSet;
import java.util.Set;

public class Grupo {
    private final String nome;
    private final Set<String> membros = new HashSet<>();

    public Grupo(String nome, String criador) {
        this.nome = nome;
        membros.add(criador);
    }

    public String getNome() {
        return nome;
    }

    public Set<String> getMembros() {
        return new HashSet<>(membros);
    }

    public boolean adicionarMembro(String usuario) {
        return membros.add(usuario);
    }

    public boolean removerMembro(String usuario) {
        return membros.remove(usuario);
    }

    public boolean contemMembro(String usuario) {
        return membros.contains(usuario);
    }
}
