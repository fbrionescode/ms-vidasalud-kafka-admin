package cl.duoc.vidasalud.kafkaadmin.dto;

import java.util.List;

public record ClusterInfoDto(
        String clusterId,
        int controladorId,
        int totalBrokers,
        List<BrokerDto> brokers
) {
}