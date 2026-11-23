# Installing the Lotse console from the tarball

This tarball holds the static `lotse-console` binary for Linux, with the web app built into it, and the scripts that run it under systemd, OpenRC or sysvinit. It works on any Linux distribution on its architecture. Where a package exists (`.deb` for Debian and Ubuntu, `.rpm` for Fedora, RHEL and SUSE, `.apk` for Alpine), the package does the steps below by itself.

The console runs on a host of its own, reaches the cluster over the native protocol and acts as the user who signed in. The guide has the whole setup: <https://pierrejochem.github.io/lotse/>.

## Every system

```sh
install -m 0755 bin/lotse-console /usr/bin/lotse-console
install -d -m 0750 /etc/lotse-console
install -m 0640 lotse-console.toml.example /etc/lotse-console/lotse-console.toml   # then edit it
```

Create a system user `lotse-console` with the home `/var/lib/lotse-console`, and the directory `/var/lib/lotse-console` owned by it with mode 0750. Make `/etc/lotse-console` and what it holds (the config, the certificate and key it serves HTTPS with, the cluster's CA, secrets) readable by that user's group.

The console serves HTTPS with the certificate and key its `[https]` names; without them it serves plain HTTP, and only on a loopback address.

## systemd

```sh
install -m 0644 systemd/lotse-console.service /usr/lib/systemd/system/
install -m 0644 systemd/lotse-console.sysusers /usr/lib/sysusers.d/lotse-console.conf
systemd-sysusers
systemctl daemon-reload
systemctl enable --now lotse-console
```

## OpenRC

```sh
install -m 0755 openrc/lotse-console.initd /etc/init.d/lotse-console
install -m 0644 openrc/lotse-console.confd /etc/conf.d/lotse-console
rc-update add lotse-console default
rc-service lotse-console start
```

The log is in `/var/log/lotse-console/lotse-console.log`.

## sysvinit

```sh
install -m 0755 sysvinit/lotse-console.init /etc/init.d/lotse-console
install -m 0644 sysvinit/lotse-console.default /etc/default/lotse-console
update-rc.d lotse-console defaults     # or chkconfig --add lotse-console
/etc/init.d/lotse-console start
```

The log is in `/var/log/lotse-console/lotse-console.log`.
