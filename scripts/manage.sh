#!/usr/bin/env bash
# Helper for common tasks. Run from repo root: ./scripts/manage.sh start
set -e
cd "$(dirname "$0")/.."
case "${1:-help}" in
  start)
    [ -f .env ] || cp .env.example .env
    docker compose up -d --build
    echo "Give it ~60s, then:"
    echo "  dashboard  http://localhost:3001"
    echo "  api docs   http://localhost:8000/docs"
    echo "  airflow    http://localhost:8080 (admin/admin)"
    echo "  kafka ui   http://localhost:8090"
    echo "  grafana    http://localhost:3000 (admin/admin)" ;;
  prod)
    docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
    echo "started with 3-broker Kafka cluster (RF=3)" ;;
  stop)   docker compose down ;;
  logs)   docker compose logs -f --tail=100 ${2:-} ;;
  status) docker compose ps ;;
  reset)  docker compose down -v --remove-orphans && docker compose up -d --build ;;
  *) echo "usage: ./scripts/manage.sh {start|prod|stop|logs|status|reset}" ;;
esac
