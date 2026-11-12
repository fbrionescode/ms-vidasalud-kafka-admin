package cl.duoc.vidasalud.kafkaadmin.exception;

public class ConfigNoPermitidaException extends RuntimeException {

    private final String campo;

    public ConfigNoPermitidaException(String campo, String mensaje) {
        super(mensaje);
        this.campo = campo;
    }

    public String getCampo() {
        return campo;
    }
}