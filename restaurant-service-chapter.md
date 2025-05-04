# Chapter 5: Project 2 - Restaurant Service

In this chapter, we'll build our first real microservice for our food delivery app: the Restaurant Service. This service will manage restaurant information, menus, and handle incoming orders.

## What You'll Learn

- How to create a Spring Boot microservice
- How to use Kafka to receive and publish messages
- How to store restaurant data
- How to build a simple REST API
- How to package the service with Docker

## Why We Need This Service

The Restaurant Service is essential because:
- It stores all restaurant information (names, locations, operating hours)
- It keeps track of menus and food items
- It receives new orders from customers
- It updates order status (accepted, preparing, ready)

## Project Setup

### Step 1: Create a Spring Boot Project

1. Go to [Spring Initializr](https://start.spring.io/)
2. Fill in the details:
   - Group: `com.foodapp`
   - Artifact: `restaurant-service`
   - Dependencies: 
     - Spring Web
     - Spring Data JPA
     - H2 Database
     - Spring for Apache Kafka
     - Lombok
3. Download and unzip the project

### Step 2: Set Up the Database Models

Create these files in the `model` package:

`Restaurant.java`:
```java
package com.foodapp.restaurantservice.model;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
import javax.persistence.CascadeType;
import java.util.List;

@Entity
@Data
public class Restaurant {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String address;
    private String phone;
    private boolean isOpen;
    
    @OneToMany(mappedBy = "restaurant", cascade = CascadeType.ALL)
    private List<MenuItem> menuItems;
}
```

`MenuItem.java`:
```java
package com.foodapp.restaurantservice.model;

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
public class MenuItem {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String description;
    private BigDecimal price;
    private boolean available;
    
    @ManyToOne
    @JoinColumn(name = "restaurant_id")
    private Restaurant restaurant;
}
```

`Order.java`:
```java
package com.foodapp.restaurantservice.model;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.ElementCollection;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "restaurant_orders") // Avoid "order" which is an SQL keyword
@Data
public class Order {
    @Id
    private String id; // Using the order ID from the order service
    private Long restaurantId;
    private String customerName;
    private String customerPhone;
    private String deliveryAddress;
    
    @ElementCollection
    private List<OrderItem> items;
    
    private BigDecimal totalAmount;
    private String status; // NEW, ACCEPTED, PREPARING, READY, PICKED_UP
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
```

`OrderItem.java`:
```java
package com.foodapp.restaurantservice.model;

import lombok.Data;

import javax.persistence.Embeddable;
import java.math.BigDecimal;

@Embeddable
@Data
public class OrderItem {
    private Long menuItemId;
    private String name;
    private int quantity;
    private BigDecimal price;
}
```

### Step 3: Create Repositories

Create these files in the `repository` package:

`RestaurantRepository.java`:
```java
package com.foodapp.restaurantservice.repository;

import com.foodapp.restaurantservice.model.Restaurant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RestaurantRepository extends JpaRepository<Restaurant, Long> {
}
```

`MenuItemRepository.java`:
```java
package com.foodapp.restaurantservice.repository;

import com.foodapp.restaurantservice.model.MenuItem;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface MenuItemRepository extends JpaRepository<MenuItem, Long> {
    List<MenuItem> findByRestaurantId(Long restaurantId);
}
```

`OrderRepository.java`:
```java
package com.foodapp.restaurantservice.repository;

import com.foodapp.restaurantservice.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface OrderRepository extends JpaRepository<Order, String> {
    List<Order> findByRestaurantId(Long restaurantId);
}
```

### Step 4: Create DTOs for Kafka Messages

Create these files in the `dto` package:

`OrderDto.java`:
```java
package com.foodapp.restaurantservice.dto;

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
package com.foodapp.restaurantservice.dto;

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
package com.foodapp.restaurantservice.dto;

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

### Step 5: Create Services

Create this file in the `service` package:

`RestaurantService.java`:
```java
package com.foodapp.restaurantservice.service;

import com.foodapp.restaurantservice.model.Restaurant;
import com.foodapp.restaurantservice.model.MenuItem;
import com.foodapp.restaurantservice.model.Order;
import com.foodapp.restaurantservice.model.OrderItem;
import com.foodapp.restaurantservice.repository.RestaurantRepository;
import com.foodapp.restaurantservice.repository.MenuItemRepository;
import com.foodapp.restaurantservice.repository.OrderRepository;
import com.foodapp.restaurantservice.dto.OrderDto;
import com.foodapp.restaurantservice.dto.OrderItemDto;
import com.foodapp.restaurantservice.dto.OrderStatusUpdateDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class RestaurantService {

    @Autowired
    private RestaurantRepository restaurantRepository;
    
    @Autowired
    private MenuItemRepository menuItemRepository;
    
    @Autowired
    private OrderRepository orderRepository;
    
    @Autowired
    private KafkaTemplate<String, OrderStatusUpdateDto> kafkaTemplate;
    
    // Restaurant methods
    public List<Restaurant> getAllRestaurants() {
        return restaurantRepository.findAll();
    }
    
    public Optional<Restaurant> getRestaurantById(Long id) {
        return restaurantRepository.findById(id);
    }
    
    public Restaurant saveRestaurant(Restaurant restaurant) {
        return restaurantRepository.save(restaurant);
    }
    
    // Menu methods
    public List<MenuItem> getMenuForRestaurant(Long restaurantId) {
        return menuItemRepository.findByRestaurantId(restaurantId);
    }
    
    public MenuItem saveMenuItem(MenuItem menuItem) {
        return menuItemRepository.save(menuItem);
    }
    
    // Order methods
    public Order processNewOrder(OrderDto orderDto) {
        Order order = new Order();
        order.setId(orderDto.getId());
        order.setRestaurantId(orderDto.getRestaurantId());
        order.setCustomerName(orderDto.getCustomerName());
        order.setCustomerPhone(orderDto.getCustomerPhone());
        order.setDeliveryAddress(orderDto.getDeliveryAddress());
        order.setTotalAmount(orderDto.getTotalAmount());
        order.setStatus("NEW");
        order.setCreatedAt(LocalDateTime.now());
        order.setUpdatedAt(LocalDateTime.now());
        
        // Convert order items
        List<OrderItem> items = orderDto.getItems().stream()
            .map(itemDto -> {
                OrderItem item = new OrderItem();
                item.setMenuItemId(itemDto.getMenuItemId());
                item.setName(itemDto.getName());
                item.setQuantity(itemDto.getQuantity());
                item.setPrice(itemDto.getPrice());
                return item;
            })
            .collect(Collectors.toList());
        
        order.setItems(items);
        
        return orderRepository.save(order);
    }
    
    public void updateOrderStatus(String orderId, String status) {
        Optional<Order> optionalOrder = orderRepository.findById(orderId);
        
        if (optionalOrder.isPresent()) {
            Order order = optionalOrder.get();
            order.setStatus(status);
            order.setUpdatedAt(LocalDateTime.now());
            orderRepository.save(order);
            
            // Send update to Kafka
            OrderStatusUpdateDto updateDto = new OrderStatusUpdateDto();
            updateDto.setOrderId(orderId);
            updateDto.setStatus(status);
            updateDto.setRestaurantId(order.getRestaurantId());
            updateDto.setUpdatedAt(order.getUpdatedAt());
            
            kafkaTemplate.send("order-updates", updateDto);
        }
    }
    
    public List<Order> getOrdersForRestaurant(Long restaurantId) {
        return orderRepository.findByRestaurantId(restaurantId);
    }
}
```

### Step 6: Create Kafka Configuration

Create this file in the `config` package:

`KafkaConfig.java`:
```java
package com.foodapp.restaurantservice.config;

import com.foodapp.restaurantservice.dto.OrderDto;
import com.foodapp.restaurantservice.dto.OrderStatusUpdateDto;
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

    // Producer configuration for order updates
    @Bean
    public ProducerFactory<String, OrderStatusUpdateDto> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, OrderStatusUpdateDto> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // Consumer configuration for new orders
    @Bean
    public ConsumerFactory<String, OrderDto> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "restaurant-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.foodapp.restaurantservice.dto");
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                new JsonDeserializer<>(OrderDto.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderDto> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
```

### Step 7: Create Kafka Consumer

Create this file in the `kafka` package:

`OrderConsumer.java`:
```java
package com.foodapp.restaurantservice.kafka;

import com.foodapp.restaurantservice.dto.OrderDto;
import com.foodapp.restaurantservice.service.RestaurantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    @Autowired
    private RestaurantService restaurantService;

    @KafkaListener(topics = "new-orders", groupId = "restaurant-group")
    public void listen(OrderDto orderDto) {
        System.out.println("Received new order: " + orderDto.getId());
        restaurantService.processNewOrder(orderDto);
    }
}
```

### Step 8: Create REST Controllers

Create these files in the `controller` package:

`RestaurantController.java`:
```java
package com.foodapp.restaurantservice.controller;

import com.foodapp.restaurantservice.model.Restaurant;
import com.foodapp.restaurantservice.model.MenuItem;
import com.foodapp.restaurantservice.service.RestaurantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/restaurants")
public class RestaurantController {

    @Autowired
    private RestaurantService restaurantService;

    @GetMapping
    public List<Restaurant> getAllRestaurants() {
        return restaurantService.getAllRestaurants();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Restaurant> getRestaurantById(@PathVariable Long id) {
        return restaurantService.getRestaurantById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public Restaurant createRestaurant(@RequestBody Restaurant restaurant) {
        return restaurantService.saveRestaurant(restaurant);
    }

    @GetMapping("/{id}/menu")
    public List<MenuItem> getRestaurantMenu(@PathVariable Long id) {
        return restaurantService.getMenuForRestaurant(id);
    }

    @PostMapping("/{id}/menu")
    public MenuItem addMenuItem(@PathVariable Long id, @RequestBody MenuItem menuItem) {
        restaurantService.getRestaurantById(id).ifPresent(menuItem::setRestaurant);
        return restaurantService.saveMenuItem(menuItem);
    }
}
```

`OrderController.java`:
```java
package com.foodapp.restaurantservice.controller;

import com.foodapp.restaurantservice.model.Order;
import com.foodapp.restaurantservice.service.RestaurantService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/restaurants/{restaurantId}/orders")
public class OrderController {

    @Autowired
    private RestaurantService restaurantService;

    @GetMapping
    public List<Order> getOrdersForRestaurant(@PathVariable Long restaurantId) {
        return restaurantService.getOrdersForRestaurant(restaurantId);
    }

    @PutMapping("/{orderId}/status")
    public void updateOrderStatus(
            @PathVariable String orderId,
            @RequestParam String status) {
        restaurantService.updateOrderStatus(orderId, status);
    }
}
```

### Step 9: Configure Application Properties

Update `application.properties`:

```properties
# Server port
server.port=8081

# Database
spring.datasource.url=jdbc:h2:mem:restaurantdb
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.h2.console.enabled=true

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=restaurant-group
```

### Step 10: Add Sample Data

Create `data.sql` in the resources folder:

```sql
-- Sample restaurants
INSERT INTO restaurant (id, name, address, phone, is_open) 
VALUES (1, 'Pizza Palace', '123 Main St', '555-1234', true);

INSERT INTO restaurant (id, name, address, phone, is_open) 
VALUES (2, 'Burger Spot', '456 Oak Ave', '555-5678', true);

-- Sample menu items for Pizza Palace
INSERT INTO menu_item (id, name, description, price, available, restaurant_id) 
VALUES (1, 'Margherita Pizza', 'Classic cheese and tomato', 9.99, true, 1);

INSERT INTO menu_item (id, name, description, price, available, restaurant_id) 
VALUES (2, 'Pepperoni Pizza', 'Pepperoni and cheese', 11.99, true, 1);

INSERT INTO menu_item (id, name, description, price, available, restaurant_id) 
VALUES (3, 'Garlic Bread', 'Toasted with garlic butter', 3.99, true, 1);

-- Sample menu items for Burger Spot
INSERT INTO menu_item (id, name, description, price, available, restaurant_id) 
VALUES (4, 'Cheeseburger', 'Beef patty with cheese', 7.99, true, 2);

INSERT INTO menu_item (id, name, description, price, available, restaurant_id) 
VALUES (5, 'Veggie Burger', 'Plant-based patty', 8.99, true, 2);

INSERT INTO menu_item (id, name, description, price, available, restaurant_id) 
VALUES (6, 'Fries', 'Crispy french fries', 2.99, true, 2);
```

### Step 11: Build and Run the Service

```bash
mvn spring-boot:run
```

## Dockerizing the Restaurant Service

Now let's package our service in a Docker container.

### Step 1: Create a Dockerfile

Create a file named `Dockerfile` in the project root:

```dockerfile
FROM openjdk:11-jre-slim

WORKDIR /app

COPY target/restaurant-service-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]
```

### Step 2: Create a Docker Compose File

Create a file named `docker-compose.yml` in the project root:

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
    build: .
    ports:
      - "8081:8081"
    depends_on:
      - kafka
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
```

### Step 3: Build the Application

```bash
mvn clean package
```

### Step 4: Build and Run with Docker Compose

```bash
docker-compose up --build
```

## Testing the Service

Once your service is running, you can test it with these commands:

### 1. Get All Restaurants

```bash
curl http://localhost:8081/restaurants
```

### 2. Get Restaurant Details

```bash
curl http://localhost:8081/restaurants/1
```

### 3. Get Restaurant Menu

```bash
curl http://localhost:8081/restaurants/1/menu
```

### 4. Update an Order Status

```bash
curl -X PUT "http://localhost:8081/restaurants/1/orders/order123/status?status=PREPARING"
```

## What We've Learned

In this chapter, we've built our first microservice for our food delivery app. We've learned:

1. How to create a Spring Boot microservice with JPA
2. How to configure Kafka producers and consumers
3. How to handle incoming messages from Kafka
4. How to send updates to Kafka topics
5. How to build REST APIs for restaurant management
6. How to containerize our service with Docker

This Restaurant Service is now ready to:
- Store and manage restaurant data
- Process incoming orders from the Order Service
- Update order status
- Send status updates to the Notification Service

In the next chapter, we'll build the Order Service, which will handle customer orders and communicate with our Restaurant Service through Kafka.

## Exercise

Try these exercises to improve your understanding:

1. Add a feature to mark a restaurant as closed/open
2. Add validation to ensure menu items have valid prices
3. Create an endpoint to search restaurants by name
4. Modify the service to handle order cancellations
5. Add logging to track all Kafka messages
