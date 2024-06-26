#!/usr/bin/env sh

gcc solaxd.c -o solaxd -lmosquitto

systemctl -q is-active solaxd  && { echo "ERROR: SolaXd service is still running. Please run \"sudo service solaxd stop\" to stop it."; exit 1; }
[ "$(id -u)" -eq 0 ] || { echo "You need to be ROOT (sudo can be used)."; exit 1; }

install -m 755 solaxd           /usr/bin/solaxd
install -m 644 solaxd.service   /lib/systemd/system/solaxd.service
install -m 644 solaxd.conf      /etc/default/solaxd
install -m 644 solaxd.logrotate /etc/logrotate.d/solaxd
install -m 644 uninstall.sh     /usr/bin/uninstall-solaxd.sh

# start the daemon/service
#systemctl enable solaxd.service
#systemctl start  solaxd.service

echo "# Edit /etc/default/solaxd to configure options."
echo
echo "# Enable and start the daemon/service on systemd based systems (e.g. Debian 8, Ubuntu 15.x, Raspbian Jessie and newer):"
echo "sudo systemctl enable solaxd.service"
echo "sudo systemctl start solaxd.service"
