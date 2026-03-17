package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class BalanceQuerier {
    private final RestTemplate restTemplate = new RestTemplate();

    public Balance query(long userId) {
        return restTemplate.getForObject(
                "http://localhost:33400/balance?userId=" + userId,
                Balance.class
        );
    }
}