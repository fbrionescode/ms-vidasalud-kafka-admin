package cl.duoc.vidasalud.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

public record TopicoDetalleDto(
        String nombre,
        int totalParticiones,
        int factorReplicacion,
        boolean protegido,
        List<ParticionDetalleDto> particiones,
        Map<String, String> configuraciones
) {
}