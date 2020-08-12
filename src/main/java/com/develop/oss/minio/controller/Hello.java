package com.develop.oss.minio.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author changchen
 * @create 2020-08-12 11:32
 */
@RestController
public class Hello {

    @GetMapping("/hello")
    public String hello(){
        return "hello World";
    }
}
