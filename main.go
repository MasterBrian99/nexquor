package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Client connected")

	// Send a welcome message to the client
	conn.Write([]byte("Hello from Go TCP server!\n"))

	// Read data from the client
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		received := scanner.Text()
		fmt.Println("Received:", received)

		// Send a response back to the client
		response := fmt.Sprintf("You said: %s\n", received)
		conn.Write([]byte(response))
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error reading from connection:", err)
	}

	fmt.Println("Client disconnected")
}

func main() {
	// Start the TCP server
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	fmt.Printf("TCP server listening on %s\n", listener.Addr())

	// Accept incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}

		// Handle the connection in a new goroutine
		go handleConnection(conn)
	}
}
