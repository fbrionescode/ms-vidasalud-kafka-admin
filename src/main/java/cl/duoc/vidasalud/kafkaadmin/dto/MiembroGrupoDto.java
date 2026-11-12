package cl.duoc.vidasalud.kafkaadmin.dto;

import java.util.List;

public record MiembroGrupoDto(
        String memberId,
        String clientId,
        String host,
        List<String> particionesAsignadas
) {
}