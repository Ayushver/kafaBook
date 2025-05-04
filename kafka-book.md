# Learn Kafka by Building a Food Delivery App

*A Step-by-Step Guide with Simple Projects Using Spring Boot, Docker, Kubernetes, Jenkins, and AWS EC2*

## What This Book Is About

This book teaches you Apache Kafka by building small, practical projects that make up a food delivery app like Zomato. You'll learn how to set up Kafka on AWS EC2, create microservices with Spring Boot, use Docker containers, manage them with Kubernetes, and automate deployment with Jenkins. Everything is explained in simple language with clear examples.

## Table of Contents

### Part 1: Getting Started
1. [What is Kafka and Why Do We Need It?](#chapter-1-what-is-kafka-and-why-do-we-need-it)
2. [Setting Up Your Computer](#chapter-2-setting-up-your-computer)
3. [Project 1: Your First Kafka App](#chapter-3-project-1-your-first-kafka-app)

### Part 2: Building Our Food App
4. [How a Food Delivery App Works](#chapter-4-how-a-food-delivery-app-works)
5. [Project 2: Restaurant Service](#chapter-5-project-2-restaurant-service)
6. [Project 3: Order Service](#chapter-6-project-3-order-service)
7. [Project 4: Delivery Tracking Service](#chapter-7-project-4-delivery-tracking-service)

### Part 3: Making It Production-Ready
8. [Project 5: Packaging with Docker](#chapter-8-project-5-packaging-with-docker)
9. [Project 6: Deploying with Kubernetes](#chapter-9-project-6-deploying-with-kubernetes)
10. [Project 7: Automating with Jenkins](#chapter-10-project-7-automating-with-jenkins)
11. [Project 8: Monitoring Your App](#chapter-11-project-8-monitoring-your-app)

## Chapter 1: What is Kafka and Why Do We Need It?

### Understanding the Problem

Imagine you're running a food delivery app like Zomato with thousands of restaurants and delivery drivers. Here are some challenges you'll face:

- Handling thousands of orders at the same time
- Notifying restaurants about new orders instantly
- Tracking where delivery drivers are in real-time
- Sending updates to customers about their orders
- Making sure nothing breaks if one part of your system fails

Traditional ways of building apps can't handle these challenges well.

### What is Kafka?

Kafka is a tool that helps solve these problems. Think of Kafka as a super-powered message system that:

1. Takes messages from one part of your app
2. Stores them safely
3. Delivers them to other parts of your app

### Why Do We Need Kafka?

Let's look at why each technology in our stack is important:

#### 1. Why Kafka?
- **Speed**: Handles millions of messages per second
- **Reliability**: Doesn't lose messages even if servers crash
- **Connection**: Connects different parts of your app without them directly depending on each other
- **History**: Keeps a record of all messages, like a backup

#### 2. Why Spring Boot?
- **Easy Development**: Makes writing Java code much simpler
- **Ready-Made Parts**: Comes with many pre-built components
- **Kafka Support**: Works well with Kafka out of the box

#### 3. Why Docker?
- **Consistency**: Your app works the same on any computer
- **Isolation**: Each part of your app is packaged separately
- **Easy Setup**: New team members can start quickly

#### 4. Why Kubernetes?
- **Scaling**: Automatically runs more copies of your app when needed
- **Healing**: Restarts parts that crash
- **Updates**: Updates your app without downtime

#### 5. Why Jenkins?
- **Automation**: Tests and deploys your code automatically
- **Consistency**: Same process every time
- **Speed**: Faster releases with fewer mistakes

#### 6. Why AWS EC2?
- **Cloud Servers**: Run your app without buying hardware
- **Flexibility**: Add more power when you need it
- **Global Reach**: Run your app close to your users

### Kafka in a Food Delivery App

Here's how Kafka helps in a food delivery app:

1. **Order Placement**:
   - Customer places an order
   - Order service sends a message to Kafka
   - Restaurant service gets the message and shows the new order

2. **Order Status Updates**:
   - Restaurant updates order status (accepted, preparing, ready)
   - Status service sends messages to Kafka
   - Customer app gets updates in real-time

3. **Delivery Tracking**:
   - Delivery app sends location updates to Kafka
   - Tracking service reads these updates
   - Customer sees where their food is in real-time

### Real Example: How Zomato Might Use Kafka

Zomato likely uses Kafka to:
- Process millions of food orders daily
- Track thousands of delivery partners
- Send notifications when food is ready or arriving
- Collect data for recommendations
- Handle peak times like weekends or dinner rush

In this book, we'll build a simplified version of these systems step by step.

## Chapter 2: Setting Up Your Computer

### What You'll Need

Before we start building, let's set up our development environment. You'll need:

- A computer with at least 8GB RAM
- Java 11 or higher
- Docker Desktop
- Git
- An AWS account (free tier is enough)
- A text editor or IDE (we recommend Visual Studio Code or IntelliJ)

### Step-by-Step Setup Guide

#### 1. Install Java
```bash
# For Ubuntu/Debian
sudo apt update
sudo apt install openjdk-11-jdk

# For macOS
brew install openjdk@11

# For Windows
# Download and install from https://adoptopenjdk.net/
```

Check your installation:
```bash
java -version
```

#### 2. Install Docker
Follow the instructions for your operating system at [Docker's website](https://docs.docker.com/get-docker/).

Check your installation:
```bash
docker --version
```

#### 3. Install Git
```bash
# For Ubuntu/Debian
sudo apt install git

# For macOS
brew install git

# For Windows
# Download and install from https://git-scm.com/download/win
```

Check your installation:
```bash
git --version
```

#### 4. AWS Account Setup
1. Go to [AWS Free Tier](https://aws.amazon.com/free/)
2. Create an account if you don't have one
3. Set up billing alerts to avoid unexpected charges

#### 5. Install Maven
```bash
# For Ubuntu/Debian
sudo apt install maven

# For macOS
brew install maven

# For Windows
# Download and install from https://maven.apache.org/download.cgi
```

Check your installation:
```bash
mvn -version
```

Now you're ready to start building your first Kafka application!

## Chapter 3: Project 1: Your First Kafka App

In this chapter, we'll build a simple messaging app using Kafka. This will help you understand the basics before we build our food delivery app.

### What We'll Build

A simple message sending application with:
- A producer that sends messages to Kafka
- A consumer that reads messages from Kafka

### Step 1: Start Kafka with Docker

Create a file named `docker-compose.yml`:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"
  
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Start Kafka:
```bash
docker-compose up -d
```

### Step 2: Create a Spring Boot Project

Create a new Spring Boot project:

1. Go to [Spring Initializr](https://start.spring.io/)
2. Add dependencies:
   - Spring for Apache Kafka
   - Spring Web
3. Download and unzip the project

### Step 3: Set Up Kafka Producer

Add this code to a new file `KafkaProducerController.java`:

```java
package com.example.kafkademo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaProducerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @PostMapping("/send")
    public String sendMessage(@RequestParam String message) {
        kafkaTemplate.send("test-topic", message);
        return "Message sent: " + message;
    }
}
```

### Step 4: Set Up Kafka Consumer

Add this code to a new file `KafkaConsumerService.java`:

```java
package com.example.kafkademo.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "test-topic", groupId = "test-group")
    public void listen(String message) {
        System.out.println("Received message: " + message);
    }
}
```

### Step 5: Configure Kafka

Update `application.properties`:

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=test-group
spring.kafka.consumer.auto-offset-reset=earliest
```

### Step 6: Run the Application

```bash
mvn spring-boot:run
```

### Step 7: Test the Application

Send a message using curl:
```bash
curl -X POST "http://localhost:8080/send?message=Hello%20Kafka"
```

Check your application console - you should see "Received message: Hello Kafka"

### What We Learned

- How to set up Kafka using Docker
- How to create a Kafka producer in Spring Boot
- How to create a Kafka consumer in Spring Boot
- How to send and receive messages

In the next chapter, we'll start building our food delivery app using what we've learned!

## Chapter 4: How a Food Delivery App Works

Before we build our app, let's understand how a food delivery app like Zomato works and how Kafka helps.

### Basic Flow of a Food Delivery App

1. **Customer Places Order**: Customer selects restaurant and food items
2. **Restaurant Accepts Order**: Restaurant staff sees and accepts the order
3. **Food Preparation**: Restaurant prepares the food
4. **Delivery Assignment**: A delivery partner is assigned
5. **Pickup**: Delivery partner picks up the food
6. **Delivery**: Food is delivered to the customer
7. **Feedback**: Customer rates the experience

### Challenges in Building a Food Delivery App

- **High Volume**: Thousands of orders happening at once
- **Real-time Updates**: Everyone needs instant updates
- **Reliability**: Can't lose orders if a server crashes
- **Independence**: Different teams work on different parts

### How Kafka Solves These Problems

Kafka acts as a central message hub that:

1. **Keeps a Record**: All events are stored safely
2. **Connects Services**: Different parts talk through Kafka
3. **Handles Load**: Works even during peak times
4. **Scales**: Grows as your business grows

### Our Architecture

We'll build these microservices:

1. **Restaurant Service**: Manages restaurant information and menus
2. **Order Service**: Handles customer orders
3. **Delivery Service**: Tracks delivery partners and assignments
4. **Notification Service**: Sends updates to users

All these services will communicate through Kafka topics:

1. `new-orders`: When customers place orders
2. `order-updates`: When order status changes
3. `delivery-locations`: Real-time delivery partner locations
4. `notifications`: Messages to send to users

### Why This Architecture?

- **Independent Development**: Teams can work separately
- **Independent Scaling**: Scale busy services without affecting others
- **Fault Isolation**: One service failing doesn't break everything
- **Technology Freedom**: Each service can use different technologies if needed

In the next chapters, we'll build each service step by step, starting with the Restaurant Service.
