package com.bnitech.apachekafkaapplicationprogramming;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserEventVO {
    private String timestamp;
    private String userAgent;
    private String colorName;
    private String userName;
}
