package cl.duoc.vidasalud.kafkaadmin.dto;

import jakarta.validation.constraints.Min;

public record AumentarParticionesRequest(
        @Min(value = 2, message = "El nuevo total de particiones debe ser mayor o igual a 2")
        int totalParticiones
) {
}