package cl.duoc.vidasalud.kafkaadmin.dto;

public record TopicoResumenDto(
        String nombre,
        int particiones,
        int replicas,
        boolean protegido
) {
}