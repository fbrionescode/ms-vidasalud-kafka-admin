package cl.duoc.vidasalud.kafkaadmin.dto;

public record ParticionLagDto(
        String topico,
        int particion,
        long offsetActual,
        long offsetFinal,
        long lag
) {
}