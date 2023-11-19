package ru.ibs.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import ru.ibs.kafka.dto.InputDto;
import ru.ibs.kafka.dto.OutputDto;
import java.time.LocalDate;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConvertorServiceImpl implements ConvertorService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(InputDto dto) {
        kafkaTemplate.send("input", dto);
    }

    @KafkaListener(
            topics = "input",
            containerFactory = "singleFactory")
    public void receive(ConsumerRecord<String, InputDto> record) {
        log.info("=> consumed {}", record.value());
        kafkaTemplate.send("output", convert(record.value()));
    }

    private OutputDto convert(InputDto dto) {
        String fullName = dto.getName() + " " + dto.getSurname();
        Integer birthYear = LocalDate.now().getYear() - dto.getAge();
        return new OutputDto(fullName, birthYear);
    }

    public void sendCallback(InputDto message) {
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send("input", message);
        future.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onSuccess(final SendResult<String, Object> message) {
                log.info("sent message= " + message + " with offset= " + message.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(final Throwable throwable) {
                log.error("unable to send message= " + message, throwable);
            }
        });
    }

    @KafkaListener(
            topics = "output",
            containerFactory = "singleFactory")
    public void receiveEvents(ConsumerRecord<String, OutputDto> record) {
        log.info("=> consumed {}", record.value());
    }

}
