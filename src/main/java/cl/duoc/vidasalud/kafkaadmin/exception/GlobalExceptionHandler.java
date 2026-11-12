package cl.duoc.vidasalud.kafkaadmin.exception;

import cl.duoc.vidasalud.kafkaadmin.dto.ErrorResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errores = new LinkedHashMap<>();
        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            errores.put(fieldError.getField(), fieldError.getDefaultMessage());
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(400, "Bad Request", "Error de validacion en la solicitud", errores));
    }

    @ExceptionHandler(ConfigNoPermitidaException.class)
    public ResponseEntity<ErrorResponse> handleConfigNoPermitida(ConfigNoPermitidaException ex) {
        log.warn("kafka-admin | config rechazada | campo={} | motivo={}", ex.getCampo(), ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(400, "Bad Request", ex.getMessage(), Map.of(ex.getCampo(), ex.getMessage())));
    }

    @ExceptionHandler(TopicoProtegidoException.class)
    public ResponseEntity<ErrorResponse> handleTopicoProtegido(TopicoProtegidoException ex) {
        log.warn("kafka-admin | intento de borrado protegido | motivo={}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(ErrorResponse.of(403, "Forbidden", ex.getMessage(), Map.of()));
    }

    @ExceptionHandler(TopicExistsException.class)
    public ResponseEntity<ErrorResponse> handleTopicExists(TopicExistsException ex) {
        log.warn("kafka-admin | topico duplicado | motivo={}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ErrorResponse.of(409, "Conflict", "El topico ya existe en el cluster", Map.of()));
    }

    @ExceptionHandler({UnknownTopicOrPartitionException.class, GroupIdNotFoundException.class})
    public ResponseEntity<ErrorResponse> handleNotFound(RuntimeException ex) {
        log.warn("kafka-admin | recurso no encontrado | motivo={}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.of(404, "Not Found", ex.getMessage(), Map.of()));
    }

    @ExceptionHandler({
        InvalidPartitionsException.class,
        InvalidConfigurationException.class,
        InvalidReplicationFactorException.class
    })
    public ResponseEntity<ErrorResponse> handleInvalidKafkaRequest(RuntimeException ex) {
        log.warn("kafka-admin | parametro invalido en Kafka | motivo={}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(400, "Bad Request", ex.getMessage(), Map.of()));
    }

    @ExceptionHandler({TimeoutException.class, java.util.concurrent.TimeoutException.class})
    public ResponseEntity<ErrorResponse> handleTimeout(Exception ex) {
        log.error("kafka-admin | timeout comunicando con brokers | motivo={}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(ErrorResponse.of(502, "Bad Gateway", "Tiempo de espera agotado al comunicarse con el cluster Kafka", Map.of()));
    }

    @ExceptionHandler(ExecutionException.class)
    public ResponseEntity<ErrorResponse> handleExecutionException(ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof TopicExistsException tee) {
            return handleTopicExists(tee);
        }
        if (cause instanceof UnknownTopicOrPartitionException utpe) {
            return handleNotFound(utpe);
        }
        if (cause instanceof GroupIdNotFoundException ginfe) {
            return handleNotFound(ginfe);
        }
        if (cause instanceof InvalidPartitionsException
                || cause instanceof InvalidConfigurationException
                || cause instanceof InvalidReplicationFactorException) {
            return handleInvalidKafkaRequest((RuntimeException) cause);
        }
        if (cause instanceof TimeoutException te) {
            return handleTimeout(te);
        }
        log.error("kafka-admin | error en operacion de AdminClient", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of(500, "Internal Server Error", cause != null ? cause.getMessage() : ex.getMessage(), Map.of()));
    }
}