package br.ufs.dcomp.ChatRabbitMQ;

import java.util.HashMap;
import java.util.Map;

public class GroupService {
    private final Map<String, Grupo> grupos = new HashMap<>(); //mapa para armazenar os grupos criados, chave é o nome e valor é o objeto

    //cria o grupo, e o synchronized garante o uso por apenas uma thread por vez, e tambem adiciona o criador como membro
    public synchronized String criarGrupo(String nome, String criador) { 
        if (nome == null || nome.isBlank()) {
            return "Nome do grupo nao deve ser vazio";
        }

        if (grupos.containsKey(nome)) {
            return "O grupo " + nome + " ja existe";
        }

        grupos.put(nome, new Grupo(nome, criador));

        return "Grupo " + nome + " criado. Voce ja é membro.";
    }

    public synchronized String adicionarUsuario(String usuario, String nomeGrupo) { //adiciona usuario a um grupo
        Grupo grupo = grupos.get(nomeGrupo);

        if (usuario == null || usuario.isBlank()) {
            return "O nome do usuario nao pode ser vazio.";
        }

        if (grupo == null) {
            return "O grupo " + nomeGrupo + " nao existe";
        }

        if (!grupo.adicionarMembro(usuario)) {
            return "O usuario ja faz parte do grupo";
        }

        return usuario + " adicionado ao grupo " + nomeGrupo + " com sucesso";
    }

    public synchronized String removerUsuario(String usuario, String nomeGrupo) { //remove usuario de um grupo
        Grupo grupo = grupos.get(nomeGrupo);

        if (usuario == null || usuario.isBlank()) {
            return "O nome do usuario nao pode ser vazio.";
        }

        if (grupo == null) {
            return "O grupo " + nomeGrupo + " nao existe";
        }

        if (!grupo.removerMembro(usuario)) {
            return "O usuario nao faz parte do grupo";
        }

        return usuario + " removido do grupo " + nomeGrupo + " com sucesso";
    }

    public synchronized String removerGrupo(String nomeGrupo) { //apaga um grupo caso exista
        if(grupos.remove(nomeGrupo) == null) {
            return "O grupo " + nomeGrupo + " nao existe";
        }

        return "Grupo " + nomeGrupo + " removido com sucesso";
    }

    public synchronized Grupo buscarGrupo(String nomeGrupo) { //retorna grupo correspondente ao nome informado
        return grupos.get(nomeGrupo);
    }

    public synchronized boolean grupoExiste(String nomeGrupo) { //checa se existe um grupo com o nome informado
        return grupos.containsKey(nomeGrupo);
    }
}