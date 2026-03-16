#!/bin/bash

set -e

PROJECT_DIR="/home/ubuntu/environment/fiap-dep-payments-pipeline"
DATASETS_DIR="$PROJECT_DIR/datasets"

echo "Going to project directory..."
cd "$PROJECT_DIR"

echo "Creating datasets directory..."
mkdir -p "$DATASETS_DIR"

cd "$DATASETS_DIR"

echo "Cloning payment dataset repository..."
git clone https://github.com/infobarbosa/dataset-json-pagamentos.git

echo "Cloning orders dataset repository..."
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git

echo ""
echo "Datasets successfully downloaded."
echo ""

echo "Payments dataset:"
echo "$DATASETS_DIR/dataset-json-pagamentos/data/pagamentos"

echo ""
echo "Orders dataset:"
echo "$DATASETS_DIR/datasets-csv-pedidos/data/pedidos"