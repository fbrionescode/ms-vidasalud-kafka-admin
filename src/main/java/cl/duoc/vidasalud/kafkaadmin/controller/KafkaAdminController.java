package cl.duoc.vidasalud.kafkaadmin.controller;

import cl.duoc.vidasalud.kafkaadmin.dto.ActualizarConfigRequest;
import cl.duoc.vidasalud.kafkaadmin.dto.AumentarParticionesRequest;
import cl.duoc.vidasalud.kafkaadmin.dto.ClusterInfoDto;
import cl.duoc.vidasalud.kafkaadmin.dto.ConsumerGroupDetalleDto;
import cl.duoc.vidasalud.kafkaadmin.dto.ConsumerGroupResumenDto;
import cl.duoc.vidasalud.kafkaadmin.dto.CrearTopicoRequest;
import cl.duoc.vidasalud.kafkaadmin.dto.TopicoDetalleDto;
import cl.duoc.vidasalud.kafkaadmin.dto.TopicoResumenDto;
import cl.duoc.vidasalud.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping("/cluster")
    public ResponseEntity<ClusterInfoDto> obtenerCluster() throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.obtenerCluster());
    }

    @GetMapping("/topicos")
    public ResponseEntity<List<TopicoResumenDto>> listarTopicos() throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.listarTopicos());
    }

    @GetMapping("/topicos/{nombre}")
    public ResponseEntity<TopicoDetalleDto> obtenerDetalleTopico(@PathVariable String nombre)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.obtenerDetalleTopico(nombre));
    }

    @PostMapping("/topicos")
    public ResponseEntity<TopicoDetalleDto> crearTopico(@Valid @RequestBody CrearTopicoRequest request)
            throws ExecutionException, InterruptedException {
        TopicoDetalleDto creado = kafkaAdminService.crearTopico(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(creado);
    }

    @PatchMapping("/topicos/{nombre}/config")
    public ResponseEntity<TopicoDetalleDto> actualizarConfigTopico(
            @PathVariable String nombre,
            @Valid @RequestBody ActualizarConfigRequest request) throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.actualizarConfigTopico(nombre, request));
    }

    @PostMapping("/topicos/{nombre}/particiones")
    public ResponseEntity<TopicoDetalleDto> aumentarParticiones(
            @PathVariable String nombre,
            @Valid @RequestBody AumentarParticionesRequest request) throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.aumentarParticiones(nombre, request));
    }

    @DeleteMapping("/topicos/{nombre}")
    public ResponseEntity< Map<String, String> > eliminarTopico(@PathVariable String nombre)
            throws ExecutionException, InterruptedException {
        kafkaAdminService.eliminarTopico(nombre);
        return ResponseEntity.ok(Map.of(
                "mensaje", "Topico eliminado correctamente",
                "topico", nombre
        ));
    }

    @GetMapping("/grupos")
    public ResponseEntity<List<ConsumerGroupResumenDto>> listarGrupos()
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.listarGrupos());
    }

    @GetMapping("/grupos/{id}")
    public ResponseEntity<ConsumerGroupDetalleDto> obtenerDetalleGrupo(@PathVariable("id") String groupId)
            throws ExecutionException, InterruptedException {
        return ResponseEntity.ok(kafkaAdminService.obtenerDetalleGrupo(groupId));
    }
}