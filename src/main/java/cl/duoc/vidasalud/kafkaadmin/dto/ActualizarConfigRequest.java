package cl.duoc.vidasalud.kafkaadmin.dto;

import jakarta.validation.constraints.NotEmpty;
import java.util.Map;

public record ActualizarConfigRequest(
        @NotEmpty(message = "Debe enviar al menos una configuracion para modificar")
        Map<String, String> configs
) {
}