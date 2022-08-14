package com.bnitech.apachekafkaapplicationprogramming.producer;

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

@Slf4j
@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*")
@RequiredArgsConstructor
public class ProduceController {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/api/select")
    public void selectColor(@RequestHeader("user-agent") String userAgentName,
                            @RequestParam(value = "color") String colorName,
                            @RequestParam(value = "user") String userName) {

        String jsonColorLog = new Gson().toJson(UserEventVO.builder()
                                                     .timestamp(
                                                             new SimpleDateFormat("yyyy-MM-dd'T'HH:mm::ss.SSSZZ").format(LocalDateTime.now())
                                                     )
                                                     .userAgent(userAgentName)
                                                     .colorName(colorName)
                                                     .userName(userName)
                                                     .build());

        kafkaTemplate.send("select-color", jsonColorLog).addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info(result.toString());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error(ex.getMessage(), ex);
            }

        });
    }
}
