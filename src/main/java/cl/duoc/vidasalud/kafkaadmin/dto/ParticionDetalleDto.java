package cl.duoc.vidasalud.kafkaadmin.dto;

import java.util.List;

public record ParticionDetalleDto(
        int particion,
        int liderId,
        List<Integer> replicas,
        List<Integer> isr
) {
}