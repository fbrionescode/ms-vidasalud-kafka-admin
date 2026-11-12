package cl.duoc.vidasalud.kafkaadmin.dto;

import java.time.Instant;
import java.util.Map;

public record ErrorResponse(
        String timestamp,
        int status,
        String error,
        String mensaje,
        Map<String, String> detalles
) {
    public static ErrorResponse of(int status, String error, String mensaje, Map<String, String> detalles) {
        return new ErrorResponse(Instant.now().toString(), status, error, mensaje, detalles);
    }
}