#!/bin/bash

# Cleanup existing containers, images, and networks
echo "Cleaning up existing containers, images, and networks..."
docker-compose down --rmi all --remove-orphans
docker system prune -f

# Start the services
echo "Starting services..."
docker-compose up --build
