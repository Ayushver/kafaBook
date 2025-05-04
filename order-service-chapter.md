# Chapter 6: Project 3 - Order Service

In this chapter, we'll build the Order Service - the heart of our food delivery application. This service will handle customer orders, communicate with the Restaurant Service, and track order status throughout the delivery process.

## What You'll Learn

- How to create a Spring Boot microservice for order processing
- How to communicate with other services using Kafka
- How to implement a payment simulation
- How to track order status throughout its lifecycle
- How to build RESTful APIs for order management

## Why We Need This Service

The Order Service is crucial because:
- It's the central hub for customer orders
- It handles the entire order lifecycle from creation to delivery
- It manages payment processing
- It communicates order details to restaurants
- It provides order status to customers

## Project Setup

### Step 1: Create a Spring Boot Project

1. Go to [Spring Initializr](https://start.spring.io/)
2. Fill in the details:
   - Group: `com.foodapp`
   - Artifact: `order-service`
   - Dependencies: 
     - Spring Web
     - Spring Data JPA
     - H2 Database
     - Spring for Apache Kafka
     - Lombok
3. Download and unzip the project

### Step 2: Set Up the Database Models

Create these files in the `model` package:

`Order.java`:
```java
package com.foodapp.orderservice.model;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
import javax.persistence.CascadeType;
import javax.persistence.Enumerated;
import javax.persistence.EnumType;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "customer_orders") // Avoid "order" which is an SQL keyword
@Data
public class Order {
    @Id
    private String id;
    
    private Long customerId;
    private String customerName;
    private String customerPhone;
    private String deliveryAddress;
    
    private Long restaurantId;
    private String restaurantName;
    
    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<OrderItem> items;
    
    private BigDecimal subtotal;
    private BigDecimal deliveryFee;
    private BigDecimal tax;
    private BigDecimal totalAmount;
    
    @Enumerated(EnumType.STRING)
    private OrderStatus status;
    
    @Enumerated(EnumType.STRING)
    private PaymentStatus paymentStatus;
    
    private String paymentMethod;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    
    // Helper method to generate ID
    public static String generateId() {
        return UUID.randomUUID().toString();
    }
    
    // Define order statuses
    public enum OrderStatus {
        CREATED,
        CONFIRMED,
        PREPARING,
        READY_FOR_PICKUP,
        OUT_FOR_DELIVERY,
        DELIVERED,
        CANCELLED
    }
    
    // Define payment statuses
    public enum PaymentStatus {
        PENDING,
        COMPLETED,
        FAILED,
        REFUNDED
    }
}
```

`OrderItem.java`:
```java
package com.foodapp.orderservice.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.ManyToOne;
import javax.persistence.JoinColumn;
import java.math.BigDecimal;

@Entity
@Data
public class OrderItem {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private Long menuItemId;
    private String name;
    private int quantity;
    private BigDecimal price;
    private BigDecimal subtotal;
    
    @ManyToOne
    @JoinColumn(name = "order_id")
    @JsonIgnore // Prevents infinite recursion in JSON serialization
    private Order order;
}
```

### Step 3: Create Repositories

Create this file in the `repository` package:

`OrderRepository.java`:
```java
package com.foodapp.orderservice.repository;

import com.foodapp.orderservice.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, String> {
    List<Order> findByCustomerId(Long customerId);
    List<Order> findByRestaurantId(Long restaurantId);
}
```

### Step 4: Create DTOs for Kafka Messages

Create these files in the `dto` package:

`OrderDto.java`:
```java
package com.foodapp.orderservice.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.util.List;

@Data
public class OrderDto {
    private String id;
    private Long restaurantId;
    private String customerName;
    private String customerPhone;
    private String deliveryAddress;
    private List<OrderItemDto> items;
    private BigDecimal totalAmount;
}
```

`OrderItemDto.java`:
```java
package com.foodapp.orderservice.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class OrderItemDto {
    private Long menuItemId;
    private String name;
    private int quantity;
    private BigDecimal price;
}
```

`OrderStatusUpdateDto.java`:
```java
package com.foodapp.orderservice.dto;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class OrderStatusUpdateDto {
    private String orderId;
    private String status;
    private Long restaurantId;
    private LocalDateTime updatedAt;
}
```

`PaymentDto.java`:
```java
package com.foodapp.orderservice.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class PaymentDto {
    private String orderId;
    private BigDecimal amount;
    private String paymentMethod;
    private boolean success;
}
```

### Step 5: Create Services

Create these files in the `service` package:

`OrderService.java`:
```java
package com.foodapp.orderservice.service;

import com.foodapp.orderservice.dto.OrderDto;
import com.foodapp.orderservice.dto.OrderItemDto;
import com.foodapp.orderservice.dto.OrderStatusUpdateDto;
import com.foodapp.orderservice.model.Order;
import com.foodapp.orderservice.model.OrderItem;
import com.foodapp.orderservice.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class OrderService {

    @Autowired
    private OrderRepository orderRepository;
    
    @Autowired
    private PaymentService paymentService;
    
    @Autowired
    private KafkaTemplate<String, OrderDto> kafkaTemplate;
    
    public Order createOrder(Order orderRequest) {
        // Generate a new ID for the order
        String orderId = Order.generateId();
        orderRequest.setId(orderId);
        
        // Set initial status
        orderRequest.setStatus(Order.OrderStatus.CREATED);
        orderRequest.setPaymentStatus(Order.PaymentStatus.PENDING);
        
        // Set timestamps
        LocalDateTime now = LocalDateTime.now();
        orderRequest.setCreatedAt(now);
        orderRequest.setUpdatedAt(now);
        
        // Calculate totals
        BigDecimal subtotal = BigDecimal.ZERO;
        for (OrderItem item : orderRequest.getItems()) {
            item.setOrder(orderRequest);
            item.setSubtotal(item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
            subtotal = subtotal.add(item.getSubtotal());
        }
        
        // Fixed delivery fee for now
        BigDecimal deliveryFee = new BigDecimal("2.99");
        
        // Calculate tax (assuming 8% tax rate)
        BigDecimal tax = subtotal.multiply(new BigDecimal("0.08"));
        
        // Set all amounts
        orderRequest.setSubtotal(subtotal);
        orderRequest.setDeliveryFee(deliveryFee);
        orderRequest.setTax(tax);
        orderRequest.setTotalAmount(subtotal.add(deliveryFee).add(tax));
        
        // Save the order
        Order savedOrder = orderRepository.save(orderRequest);
        
        // Send payment for processing
        boolean paymentSuccess = paymentService.processPayment(savedOrder);
        
        if (paymentSuccess) {
            // Update payment status
            savedOrder.setPaymentStatus(Order.PaymentStatus.COMPLETED);
            savedOrder.setStatus(Order.OrderStatus.CONFIRMED);
            savedOrder = orderRepository.save(savedOrder);
            
            // Send to restaurant service
            sendOrderToRestaurant(savedOrder);
        } else {
            // Payment failed
            savedOrder.setPaymentStatus(Order.PaymentStatus.FAILED);
            savedOrder = orderRepository.save(savedOrder);
        }
        
        return savedOrder;
    }
    
    private void sendOrderToRestaurant(Order order) {
        // Convert to DTO
        OrderDto orderDto = new OrderDto();
        orderDto.setId(order.getId());
        orderDto.setRestaurantId(order.getRestaurantId());
        orderDto.setCustomerName(order.getCustomerName());
        orderDto.setCustomerPhone(order.getCustomerPhone());
        orderDto.setDeliveryAddress(order.getDeliveryAddress());
        orderDto.setTotalAmount(order.getTotalAmount());
        
        List<OrderItemDto> itemDtos = order.getItems().stream()
            .map(item -> {
                OrderItemDto dto = new OrderItemDto();
                dto.setMenuItemId(item.getMenuItemId());
                dto.setName(item.getName());
                dto.setQuantity(item.getQuantity());
                dto.setPrice(item.getPrice());
                return dto;
            })
            .collect(Collectors.toList());
        
        orderDto.setItems(itemDtos);
        
        // Send to Kafka
        kafkaTemplate.send("new-orders", orderDto);
    }
    
    public Optional<Order> getOrderById(String id) {
        return orderRepository.findById(id);
    }
    
    public List<Order> getOrdersByCustomerId(Long customerId) {
        return orderRepository.findByCustomerId(customerId);
    }
    
    public void updateOrderStatus(String orderId, String status) {
        Optional<Order> optionalOrder = orderRepository.findById(orderId);
        
        if (optionalOrder.isPresent()) {
            Order order = optionalOrder.get();
            try {
                Order.OrderStatus newStatus = Order.OrderStatus.valueOf(status);
                order.setStatus(newStatus);
                order.setUpdatedAt(LocalDateTime.now());
                orderRepository.save(order);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid status: " + status);
            }
        } else {
            throw new IllegalArgumentException("Order not found: " + orderId);
        }
    }
    
    public void cancelOrder(String orderId) {
        Optional<Order> optionalOrder = orderRepository.findById(orderId);
        
        if (optionalOrder.isPresent()) {
            Order order = optionalOrder.get();
            
            // Only allow cancellation if not already out for delivery
            if (order.getStatus() == Order.OrderStatus.OUT_FOR_DELIVERY ||
                order.getStatus() == Order.OrderStatus.DELIVERED) {
                throw new IllegalStateException("Cannot cancel order in status: " + order.getStatus());
            }
            
            order.setStatus(Order.OrderStatus.CANCELLED);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            
            // If payment was completed, initiate refund
            if (order.getPaymentStatus() == Order.PaymentStatus.COMPLETED) {
                boolean refundSuccess = paymentService.refundPayment(order);
                
                if (refundSuccess) {
                    order.setPaymentStatus(Order.PaymentStatus.REFUNDED);
                    orderRepository.save(order);
                }
            }
        } else {
            throw new IllegalArgumentException("Order not found: " + orderId);
        }
    }
}
```

`PaymentService.java`:
```java
package com.foodapp.orderservice.service;

import com.foodapp.orderservice.dto.PaymentDto;
import com.foodapp.orderservice.model.Order;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class PaymentService {

    private Random random = new Random();
    
    // Simulate payment processing
    public boolean processPayment(Order order) {
        // In a real application, this would integrate with a payment gateway
        // For demo purposes, we'll succeed 90% of the time
        boolean success = random.nextInt(10) < 9;
        
        System.out.println("Processing payment for order " + order.getId() + 
                           ": " + (success ? "SUCCESS" : "FAILED"));
        
        return success;
    }
    
    // Simulate payment refund
    public boolean refundPayment(Order order) {
        // In a real application, this would integrate with a payment gateway
        // For demo purposes, we'll succeed 95% of the time
        boolean success = random.nextInt(20) < 19;
        
        System.out.println("Refunding payment for order " + order.getId() + 
                           ": " + (success ? "SUCCESS" : "FAILED"));
        
        return success;
    }
}
```

### Step 6: Create Kafka Configuration

Create this file in the `config` package:

`KafkaConfig.java`:
```java
package com.foodapp.orderservice.config;

import com.foodapp.orderservice.dto.OrderDto;
import com.foodapp.orderservice.dto.OrderStatusUpdateDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    // Producer configuration for new orders
    @Bean
    public ProducerFactory<String, OrderDto> orderProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, OrderDto> kafkaTemplate() {
        return new KafkaTemplate<>(orderProducerFactory());
    }

    // Consumer configuration for order status updates
    @Bean
    public ConsumerFactory<String, OrderStatusUpdateDto> orderStatusConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.foodapp.orderservice.dto");
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                new JsonDeserializer<>(OrderStatusUpdateDto.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderStatusUpdateDto> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderStatusUpdateDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderStatusConsumerFactory());
        return factory;
    }
}
```

### Step 7: Create Kafka Consumer

Create this file in the `kafka` package:

`OrderStatusConsumer.java`:
```java
package com.foodapp.orderservice.kafka;

import com.foodapp.orderservice.dto.OrderStatusUpdateDto;
import com.foodapp.orderservice.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderStatusConsumer {

    @Autowired
    private OrderService orderService;

    @KafkaListener(topics = "order-updates", groupId = "order-group")
    public void listen(OrderStatusUpdateDto statusUpdate) {
        System.out.println("Received status update for order: " + statusUpdate.getOrderId() + 
                           ", new status: " + statusUpdate.getStatus());
        
        // Map the status from restaurant service to our order status
        String mappedStatus;
        switch (statusUpdate.getStatus()) {
            case "ACCEPTED":
                mappedStatus = "CONFIRMED";
                break;
            case "PREPARING":
                mappedStatus = "PREPARING";
                break;
            case "READY":
                mappedStatus = "READY_FOR_PICKUP";
                break;
            default:
                mappedStatus = statusUpdate.getStatus();
        }
        
        // Update our order
        try {
            orderService.updateOrderStatus(statusUpdate.getOrderId(), mappedStatus);
        } catch (Exception e) {
            System.out.println("Error updating order status: " + e.getMessage());
        }
    }
}
```

### Step 8: Create REST Controllers

Create this file in the `controller` package:

`OrderController.java`:
```java
package com.foodapp.orderservice.controller;

import com.foodapp.orderservice.model.Order;
import com.foodapp.orderservice.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        try {
            Order createdOrder = orderService.createOrder(order);
            return new ResponseEntity<>(createdOrder, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> getOrderById(@PathVariable String id) {
        return orderService.getOrderById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/customer/{customerId}")
    public List<Order> getOrdersByCustomerId(@PathVariable Long customerId) {
        return orderService.getOrdersByCustomerId(customerId);
    }

    @PutMapping("/{id}/cancel")
    public ResponseEntity<Void> cancelOrder(@PathVariable String id) {
        try {
            orderService.cancelOrder(id);
            return ResponseEntity.ok().build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().build();
        }
    }
}
```

### Step 9: Configure Application Properties

Update `application.properties`:

```properties
# Server port
server.port=8082

# Database
spring.datasource.url=jdbc:h2:mem:orderdb
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.h2.console.enabled=true

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=order-group
```

### Step 10: Dockerizing the Order Service

Create a file named `Dockerfile` in the project root:

```dockerfile
FROM openjdk:11-jre-slim

WORKDIR /app

COPY target/order-service-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "app.jar"]
```

### Step 11: Update Docker Compose File

Create a file named `docker-compose.yml` in the project root (or update if you're combining services):

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
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  
  restaurant-service:
    build: ../restaurant-service
    ports:
      - "8081:8081"
    depends_on:
      - kafka
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092

  order-service:
    build: .
    ports:
      - "8082:8082"
    depends_on:
      - kafka
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
```

### Step 12: Build and Run the Service

```bash
mvn clean package
docker-compose up --build
```

## Testing the Order Service

Once your service is running, you can test it with these commands:

### 1. Create a New Order

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "customerId": 1,
  "customerName": "John Doe",
  "customerPhone": "555-1234",
  "deliveryAddress": "123 Main St",
  "restaurantId": 1,
  "restaurantName": "Pizza Palace",
  "paymentMethod": "CREDIT_CARD",
  "items": [
    {
      "menuItemId": 1,
      "name": "Margherita Pizza",
      "quantity": 2,
      "price": 9.99
    },
    {
      "menuItemId": 3,
      "name": "Garlic Bread",
      "quantity": 1,
      "price": 3.99
    }
  ]
}' http://localhost:8082/orders
```

### 2. Get Order Details

```bash
# Replace ORDER_ID with the ID from the previous response
curl http://localhost:8082/orders/ORDER_ID
```

### 3. Get Customer Orders

```bash
curl http://localhost:8082/orders/customer/1
```

### 4. Cancel an Order

```bash
# Replace ORDER_ID with the ID from the previous response
curl -X PUT http://localhost:8082/orders/ORDER_ID/cancel
```

## How It All Works Together

When you run both the Restaurant Service and Order Service together:

1. A customer creates an order through the Order Service
2. The Order Service processes the payment
3. If successful, it sends the order to Kafka (topic: `new-orders`)
4. The Restaurant Service picks up the order from Kafka
5. When the restaurant updates the order status, it sends an update to Kafka (topic: `order-updates`)
6. The Order Service receives the status update and updates its database
7. The customer can check their order status through the Order Service

This demonstrates how Kafka enables communication between independent microservices without them having to directly call each other's APIs.

## What We've Learned

In this chapter, we've built the Order Service for our food delivery app. We've learned:

1. How to create a Spring Boot microservice for order processing
2. How to implement a payment simulation system
3. How to send order information to Kafka
4. How to consume order status updates from Kafka
5. How to build RESTful APIs for order management
6. How services can communicate asynchronously through Kafka

In the next chapter, we'll build the Delivery Tracking Service, which will handle assigning delivery partners and tracking their location in real-time.

## Exercise

Try these exercises to improve your understanding:

1. Add email or SMS notification when an order status changes
2. Implement order filtering (by date, status, etc.)
3. Add a rating system for completed orders
4. Create a dashboard endpoint that returns order statistics
5. Implement order validation to check if items are available before processing
