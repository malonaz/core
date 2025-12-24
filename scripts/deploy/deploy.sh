#!/bin/bash
set -e

echo "Building..."
plz build //server

echo "Deploying..."
ansible-playbook -i cmd/deploy/inventory.ini cmd/deploy/playbook.yml --vault-password-file /secrets/malonaz/ansible-vault-password
