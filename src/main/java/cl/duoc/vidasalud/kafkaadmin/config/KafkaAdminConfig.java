package cl.duoc.vidasalud.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaAdminConfig {

    private final KafkaAdminProperties properties;

    public KafkaAdminConfig(KafkaAdminProperties properties) {
        this.properties = properties;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, properties.getRequestTimeoutMs());
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, properties.getRequestTimeoutMs());
        return new KafkaAdmin(configs);
    }

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean
    public NewTopic topicoAtencionesEventos() {
        return TopicBuilder.name(properties.getTopicoAtenciones())
                .partitions(properties.getParticiones())
                .replicas(properties.getReplicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, properties.getMinInsyncReplicas())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, properties.getRetencionAtencionesMs())
                .build();
    }

    @Bean
    public NewTopic topicoBoxesEstado() {
        return TopicBuilder.name(properties.getTopicoBoxes())
                .partitions(properties.getParticiones())
                .replicas(properties.getReplicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, properties.getMinInsyncReplicas())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .config(TopicConfig.SEGMENT_MS_CONFIG, properties.getSegmentBoxesMs())
                .build();
    }

    @Bean
    public NewTopic topicoAtencionesEventosDlt() {
        return TopicBuilder.name(properties.getTopicoAtencionesDlt())
                .partitions(properties.getParticiones())
                .replicas(properties.getReplicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, properties.getMinInsyncReplicas())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, properties.getRetencionAtencionesMs())
                .build();
    }

    @Bean
    public NewTopic topicoBoxesEstadoDlt() {
        return TopicBuilder.name(properties.getTopicoBoxesDlt())
                .partitions(properties.getParticiones())
                .replicas(properties.getReplicas())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, properties.getMinInsyncReplicas())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, properties.getRetencionAtencionesMs())
                .build();
    }
}