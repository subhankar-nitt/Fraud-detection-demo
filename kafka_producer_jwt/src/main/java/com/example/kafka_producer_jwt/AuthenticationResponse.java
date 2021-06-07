package com.example.kafka_producer_jwt;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.context.annotation.Bean;

@AllArgsConstructor
@Data
@ToString
public class AuthenticationResponse {
    private final String jwt;
}
