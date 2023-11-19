package ru.ibs.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.ibs.kafka.dto.InputDto;
import ru.ibs.kafka.service.ConvertorServiceImpl;

@RestController
@RequiredArgsConstructor
public class KafkaController {

    private final ConvertorServiceImpl convertorService;

    @PostMapping("/produce")
    public ResponseEntity produce(@RequestBody InputDto dto) {
        convertorService.send(dto);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/callback")
    public ResponseEntity callback(@RequestBody InputDto dto) {
        convertorService.sendCallback(dto);
        return ResponseEntity.ok().build();
    }

}
