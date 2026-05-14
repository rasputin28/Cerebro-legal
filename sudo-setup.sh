#!/usr/bin/env bash
# One-time headless + power-saving setup. Run on the remote machine:
#   bash ~/scjn-scraper/sudo-setup.sh
# You'll be prompted for sudo once.
set -e

echo "==> apt update + installing tmux, tlp, htop, jq, curl"
sudo apt-get update -y
sudo apt-get install -y tmux tlp htop jq curl

echo "==> Disabling GUI on boot (multi-user.target)"
sudo systemctl set-default multi-user.target

echo "==> Enabling TLP power management"
sudo systemctl enable --now tlp.service || true

echo "==> Stopping the GUI now (this will not disconnect SSH)"
sudo systemctl isolate multi-user.target || true

echo
echo "Done. The machine will boot to a text console from now on."
echo "Revert with:  sudo systemctl set-default graphical.target && sudo reboot"
