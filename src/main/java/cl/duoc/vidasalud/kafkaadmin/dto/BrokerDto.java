package cl.duoc.vidasalud.kafkaadmin.dto;

public record BrokerDto(
        int id,
        String host,
        int port,
        String rack,
        boolean esControlador
) {
}