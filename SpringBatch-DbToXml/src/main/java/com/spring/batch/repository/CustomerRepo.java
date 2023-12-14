package com.spring.batch.repository;

import java.io.Serializable;

import org.springframework.data.jpa.repository.JpaRepository;

import com.spring.batch.entity.Customer;

public interface CustomerRepo extends JpaRepository<Customer,Serializable>{

}