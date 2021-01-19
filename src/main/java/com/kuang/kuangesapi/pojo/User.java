package com.kuang.kuangesapi.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * @program: kuang-es-api
 * @description:
 * @version: 1.0
 * @author: LiuJiaQi
 * @create: 2021-01-19 14:43
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
@Component
public class User {
    private String name;
    private int age;
}
