package cl.duoc.vidasalud.kafkaadmin.dto;

import java.util.List;

public record ConsumerGroupDetalleDto(
        String groupId,
        String estado,
        int coordinadorId,
        long lagTotal,
        List<MiembroGrupoDto> miembros,
        List<ParticionLagDto> offsetsPorParticion
) {
}