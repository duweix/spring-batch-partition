package com.example.spring_batch_partition;

import lombok.Data;

@Data
public class Transaction {
    private String username;
    private String userid;
    private String transactiondate;
    private String amount;
}
