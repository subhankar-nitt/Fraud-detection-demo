package com.example.kafka_producer_jwt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.web.bind.annotation.*;

@RestController
public class KafkaController {
    @Autowired
    private ProducerService producerService;

    @Autowired
    private MyUserDetailsService myUserDetailsService;

    @Autowired
    private jwtUtil myJwt;

    @Autowired
    private AuthenticationManager authenticationManager;

    @PostMapping("/producer")
    public void sendMessage(@RequestBody Transaction transaction ) throws JsonProcessingException {
        producerService.sendMessage(new ObjectMapper().writeValueAsString(transaction));
    }
    @GetMapping("/hello")
    public String returnHello(){
        System.out.println("hello world");
        return "<h1>Hello world</h1>";
    }

    @PostMapping("/authenticate")
    public ResponseEntity<?> createAuthenticationToken(@RequestBody AuthenticationRequest authenticationRequest) throws Exception{
        try{
            authenticationManager.authenticate(new UsernamePasswordAuthenticationToken(authenticationRequest.getUserId(),authenticationRequest.getPassword()));
        }catch (Exception ex){
            ex.printStackTrace();
        }

        final UserDetails userDetails = myUserDetailsService.loadUserByUsername(authenticationRequest.getUserId());
        final String jwt = myJwt.generateToken(userDetails);
        System.out.println(jwt);
        return ResponseEntity.ok(new AuthenticationResponse(jwt));

    }


}
