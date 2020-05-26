package com.springboot.model;

import java.io.Serializable;

public class Job implements Serializable {

	private static final long serialVersionUID = 1L;

	private String jobId;
	private String firstName;
	private String lastName;
	private Address address;

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String studentId) {
		this.jobId = studentId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}
}
