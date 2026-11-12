package cl.duoc.vidasalud.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.Map;

public record CrearTopicoRequest(
        @NotBlank(message = "El nombre del topico es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre solo puede contener letras, numeros, puntos, guiones y guiones bajos")
        String nombre,

        @Min(value = 1, message = "El numero de particiones debe ser al menos 1")
        int particiones,

        @Min(value = 1, message = "El factor de replicacion debe ser entre 1 y 3")
        @Max(value = 3, message = "El factor de replicacion debe ser entre 1 y 3")
        short replicas,

        Map<String, String> configs
) {
}