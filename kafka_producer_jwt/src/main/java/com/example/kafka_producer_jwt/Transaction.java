package com.example.kafka_producer_jwt;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
@NoArgsConstructor
public class Transaction {

    private String ccNo;
    private String data;
    private String amount;

}
