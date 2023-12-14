package com.spring.batch.entity;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name="customers")
@Data
@AllArgsConstructor
@NoArgsConstructor
@XStreamAlias("Customer")
public class Customer {

	@Id
	@Column(name="CUSTOMER_ID")
	private int id;
	
	@Column(name="FIRST_NAME")	
	private String firstName;	
	
	@Column(name="LAST_NAME")
	private String lastName;
	
	@Column(name="EMAIL")
	private String email;
	
	@Column(name="GENDER")
	private String gender;
	
	@Column(name="CONTACT_NO")
	private String contactNo;
	
	@Column(name="COUNTRY")
	private String country;
	
	@Column(name="DOB")
	private String dob;
}
