package cl.duoc.vidasalud.kafkaadmin.exception;

public class TopicoProtegidoException extends RuntimeException {

    public TopicoProtegidoException(String nombreTopico) {
        super("No se permite eliminar el topico protegido del sistema: " + nombreTopico);
    }
}