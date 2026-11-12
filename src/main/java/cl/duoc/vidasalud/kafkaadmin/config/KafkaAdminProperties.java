package cl.duoc.vidasalud.kafkaadmin.config;

import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "vidasalud.kafka")
public class KafkaAdminProperties {

    private String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";
    private int requestTimeoutMs = 5000;
    private String topicoAtenciones = "vidasalud.atenciones.eventos";
    private String topicoAtencionesDlt = "vidasalud.atenciones.eventos.DLT";
    private String topicoBoxes = "vidasalud.boxes.estado";
    private String topicoBoxesDlt = "vidasalud.boxes.estado.DLT";
    private int particiones = 3;
    private int replicas = 3;
    private String minInsyncReplicas = "2";
    private String retencionAtencionesMs = "604800000";
    private String segmentBoxesMs = "600000";

    public Set<String> getTopicosProtegidos() {
        return Set.of(topicoAtenciones, topicoAtencionesDlt, topicoBoxes, topicoBoxesDlt);
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public String getTopicoAtenciones() {
        return topicoAtenciones;
    }

    public void setTopicoAtenciones(String topicoAtenciones) {
        this.topicoAtenciones = topicoAtenciones;
    }

    public String getTopicoAtencionesDlt() {
        return topicoAtencionesDlt;
    }

    public void setTopicoAtencionesDlt(String topicoAtencionesDlt) {
        this.topicoAtencionesDlt = topicoAtencionesDlt;
    }

    public String getTopicoBoxes() {
        return topicoBoxes;
    }

    public void setTopicoBoxes(String topicoBoxes) {
        this.topicoBoxes = topicoBoxes;
    }

    public String getTopicoBoxesDlt() {
        return topicoBoxesDlt;
    }

    public void setTopicoBoxesDlt(String topicoBoxesDlt) {
        this.topicoBoxesDlt = topicoBoxesDlt;
    }

    public int getParticiones() {
        return particiones;
    }

    public void setParticiones(int particiones) {
        this.particiones = particiones;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public String getMinInsyncReplicas() {
        return minInsyncReplicas;
    }

    public void setMinInsyncReplicas(String minInsyncReplicas) {
        this.minInsyncReplicas = minInsyncReplicas;
    }

    public String getRetencionAtencionesMs() {
        return retencionAtencionesMs;
    }

    public void setRetencionAtencionesMs(String retencionAtencionesMs) {
        this.retencionAtencionesMs = retencionAtencionesMs;
    }

    public String getSegmentBoxesMs() {
        return segmentBoxesMs;
    }

    public void setSegmentBoxesMs(String segmentBoxesMs) {
        this.segmentBoxesMs = segmentBoxesMs;
    }
}