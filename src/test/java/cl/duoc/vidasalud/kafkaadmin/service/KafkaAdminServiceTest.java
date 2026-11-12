package cl.duoc.vidasalud.kafkaadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.vidasalud.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.vidasalud.kafkaadmin.dto.ActualizarConfigRequest;
import cl.duoc.vidasalud.kafkaadmin.dto.ConsumerGroupDetalleDto;
import cl.duoc.vidasalud.kafkaadmin.dto.CrearTopicoRequest;
import cl.duoc.vidasalud.kafkaadmin.dto.TopicoDetalleDto;
import cl.duoc.vidasalud.kafkaadmin.exception.ConfigNoPermitidaException;
import cl.duoc.vidasalud.kafkaadmin.exception.TopicoProtegidoException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaAdminServiceTest {

    @Mock
    private AdminClient adminClient;

    private KafkaAdminProperties properties;
    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        properties = new KafkaAdminProperties();
        service = new KafkaAdminService(adminClient, properties);
    }

    @Test
    void crearTopicoExitoso() throws Exception {
        String nombre = "vidasalud.pruebas.eventos";
        CrearTopicoRequest request = new CrearTopicoRequest(
                nombre, 3, (short) 3, Map.of("retention.ms", "604800000")
        );

        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        when(createResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

        mockDescribeTopicoYConfig(nombre, "604800000");

        TopicoDetalleDto resultado = service.crearTopico(request);

        assertEquals(nombre, resultado.nombre());
        assertEquals(3, resultado.totalParticiones());
        assertEquals(3, resultado.factorReplicacion());
        assertFalse(resultado.protegido());
        assertEquals("604800000", resultado.configuraciones().get("retention.ms"));
    }

    @Test
    void crearTopicoDuplicadoLanzaTopicExistsException() {
        String nombre = "vidasalud.atenciones.eventos";
        CrearTopicoRequest request = new CrearTopicoRequest(nombre, 3, (short) 3, Map.of());

        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        KafkaFutureImpl<Void> failedFuture = new KafkaFutureImpl<>();
        failedFuture.completeExceptionally(new TopicExistsException("El topico ya existe"));
        when(createResult.all()).thenReturn(failedFuture);
        when(adminClient.createTopics(anyCollection())).thenReturn(createResult);

        ExecutionException ex = assertThrows(ExecutionException.class, () -> service.crearTopico(request));
        assertTrue(ex.getCause() instanceof TopicExistsException);
    }

    @Test
    void eliminarTopicoProtegidoLanzaTopicoProtegidoException() {
        assertThrows(
                TopicoProtegidoException.class,
                () -> service.eliminarTopico("vidasalud.atenciones.eventos")
        );
        verify(adminClient, never()).deleteTopics(anyCollection());
    }

    @Test
    void obtenerDetalleTopicoInexistenteLanzaUnknownTopicOrPartitionException() {
        String nombre = "topico.inexistente";
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        KafkaFutureImpl< Map<String, TopicDescription> > failedFuture = new KafkaFutureImpl<>();
        failedFuture.completeExceptionally(new UnknownTopicOrPartitionException("Topico no existe"));
        when(describeResult.allTopicNames()).thenReturn(failedFuture);
        when(adminClient.describeTopics(eq(List.of(nombre)))).thenReturn(describeResult);

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> service.obtenerDetalleTopico(nombre)
        );
        assertTrue(ex.getCause() instanceof UnknownTopicOrPartitionException);
    }

    @Test
    void actualizarConfigTopicoPermitidaExitoso() throws Exception {
        String nombre = "vidasalud.atenciones.eventos";
        ActualizarConfigRequest request = new ActualizarConfigRequest(Map.of("retention.ms", "1209600000"));

        AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
        when(alterResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.incrementalAlterConfigs(anyMap())).thenReturn(alterResult);

        mockDescribeTopicoYConfig(nombre, "1209600000");

        TopicoDetalleDto resultado = service.actualizarConfigTopico(nombre, request);

        assertEquals("1209600000", resultado.configuraciones().get("retention.ms"));
        assertTrue(resultado.protegido());
    }

    @Test
    void actualizarConfigFueraDeListaBlancaLanzaConfigNoPermitidaException() {
        ActualizarConfigRequest request = new ActualizarConfigRequest(
                Map.of("auto.create.topics.enable", "true")
        );

        ConfigNoPermitidaException ex = assertThrows(
                ConfigNoPermitidaException.class,
                () -> service.actualizarConfigTopico("vidasalud.atenciones.eventos", request)
        );
        assertEquals("auto.create.topics.enable", ex.getCampo());
        verify(adminClient, never()).incrementalAlterConfigs(anyMap());
    }

    @Test
    void obtenerDetalleGrupoCalculaLagCorrectamente() throws Exception {
        String groupId = "vidasalud-analitica";
        TopicPartition tp0 = new TopicPartition("vidasalud.atenciones.eventos", 0);
        TopicPartition tp1 = new TopicPartition("vidasalud.atenciones.eventos", 1);

        Node coordinator = new Node(1, "kafka1", 29092);
        MemberDescription miembro = new MemberDescription(
                "member-1",
                "client-1",
                "/172.18.0.5",
                new MemberAssignment(Set.of(tp0, tp1))
        );
        ConsumerGroupDescription groupDesc = new ConsumerGroupDescription(
                groupId,
                false,
                List.of(miembro),
                "range",
                ConsumerGroupState.STABLE,
                coordinator
        );

        DescribeConsumerGroupsResult describeGroupResult = mock(DescribeConsumerGroupsResult.class);
        when(describeGroupResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, groupDesc)));
        when(adminClient.describeConsumerGroups(eq(List.of(groupId)))).thenReturn(describeGroupResult);

        ListConsumerGroupOffsetsResult offsetsGrupoResult = mock(ListConsumerGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> offsetsActuales = Map.of(
                tp0, new OffsetAndMetadata(10L),
                tp1, new OffsetAndMetadata(20L)
        );
        when(offsetsGrupoResult.partitionsToOffsetAndMetadata())
                .thenReturn(KafkaFuture.completedFuture(offsetsActuales));
        when(adminClient.listConsumerGroupOffsets(eq(groupId))).thenReturn(offsetsGrupoResult);

        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        Map<TopicPartition, ListOffsetsResultInfo> offsetsFinales = Map.of(
                tp0, new ListOffsetsResultInfo(13L, -1L, java.util.Optional.empty()),
                tp1, new ListOffsetsResultInfo(20L, -1L, java.util.Optional.empty())
        );
        when(listOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(offsetsFinales));
        when(adminClient.listOffsets(anyMap())).thenReturn(listOffsetsResult);

        ConsumerGroupDetalleDto detalle = service.obtenerDetalleGrupo(groupId);

        assertEquals(groupId, detalle.groupId());
        assertEquals("STABLE", detalle.estado());
        assertEquals(3L, detalle.lagTotal());
        assertEquals(2, detalle.offsetsPorParticion().size());
        assertEquals(3L, detalle.offsetsPorParticion().get(0).lag());
        assertEquals(0L, detalle.offsetsPorParticion().get(1).lag());
    }

    private void mockDescribeTopicoYConfig(String nombre, String retentionMs) {
        Node n1 = new Node(1, "kafka1", 29092);
        Node n2 = new Node(2, "kafka2", 29092);
        Node n3 = new Node(3, "kafka3", 29092);
        List<Node> replicas = List.of(n1, n2, n3);

        List<TopicPartitionInfo> partitions = List.of(
                new TopicPartitionInfo(0, n1, replicas, replicas),
                new TopicPartitionInfo(1, n2, replicas, replicas),
                new TopicPartitionInfo(2, n3, replicas, replicas)
        );
        TopicDescription desc = new TopicDescription(nombre, false, partitions);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(nombre, desc)));
        when(adminClient.describeTopics(eq(List.of(nombre)))).thenReturn(describeTopicsResult);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = new Config(List.of(
                new ConfigEntry("retention.ms", retentionMs),
                new ConfigEntry("cleanup.policy", "delete"),
                new ConfigEntry("min.insync.replicas", "2")
        ));
        DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
        when(describeConfigsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, config)));
        when(adminClient.describeConfigs(eq(List.of(resource)))).thenReturn(describeConfigsResult);
    }
}