#!/bin/sh
set -eu
kernel=$(find /lib/modules -mindepth 1 -maxdepth 1 -type d | head -1 | xargs basename)
mkdir -p /guest/bin /guest/dev /guest/proc /guest/sys /guest/tmp /guest/data /guest/run/cursus-e2e-faults
cp /bin/busybox /guest/bin/busybox
for app in sh mount mkdir sleep cat poweroff modprobe ifconfig; do ln -s busybox /guest/bin/$app; done
cp /broker-tests /guest/broker-tests
cp /guest-init /guest/init
chmod +x /guest/init
for module in virtio_pci virtio_blk crc32c_generic ext4 ipv6; do
    modprobe --show-depends -S "$kernel" "$module" | while read -r command path rest; do
        if [ "$command" = insmod ]; then cp --parents "$path" /guest; fi
    done
done
mkdir -p "/guest/lib/modules/$kernel"
cp /lib/modules/"$kernel"/modules.* "/guest/lib/modules/$kernel/"
cp /boot/vmlinuz-"$kernel" /kernel
cd /guest
find . -print0 | cpio --null -o --format=newc | gzip -1 > /initramfs.gz
