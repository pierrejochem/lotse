# Installing Lotse from the tarball

This tarball holds the static `lotse` binary for Linux and the scripts that run a node under systemd, OpenRC or sysvinit. It works on any Linux distribution on its architecture. Where a package exists (`.deb` for Debian and Ubuntu, `.rpm` for Fedora, RHEL and SUSE, `.apk` for Alpine), the package does the steps below by itself.

The guide has the whole setup: <https://pierrejochem.github.io/lotse/>.

## Every system

```sh
install -m 0755 bin/lotse /usr/bin/lotse
install -d -m 0755 /etc/lotse
install -m 0644 cluster.toml.example /etc/lotse/cluster.toml   # then edit it
lotse check --config /etc/lotse/cluster.toml
lotse certs --config /etc/lotse/cluster.toml
```

Create a system user `lotse` with the home `/var/lib/lotse`, and the directory `/var/lib/lotse` owned by it with mode 0750. Keys under `/etc/lotse` must be readable by that user.

## systemd

```sh
install -m 0644 systemd/lotse@.service /usr/lib/systemd/system/
install -m 0644 systemd/lotse.sysusers /usr/lib/sysusers.d/lotse.conf
systemd-sysusers
systemctl daemon-reload
systemctl enable --now lotse@2        # the node's id in the cluster file
```

## OpenRC

```sh
install -m 0755 openrc/lotse.initd /etc/init.d/lotse
install -m 0644 openrc/lotse.confd /etc/conf.d/lotse
ln -s lotse /etc/init.d/lotse.2        # the node's id in the cluster file
rc-update add lotse.2 default
rc-service lotse.2 start
```

The log is in `/var/log/lotse/lotse.2.log`.

## sysvinit

```sh
install -m 0755 sysvinit/lotse.init /etc/init.d/lotse
install -m 0644 sysvinit/lotse.default /etc/default/lotse
# set LOTSE_NODE in /etc/default/lotse to the node's id
update-rc.d lotse defaults             # or chkconfig --add lotse
/etc/init.d/lotse start
```

The script uses `start-stop-daemon`. The log is in `/var/log/lotse/lotse.log`.

## Checking the download

Every release has a `SHA256SUMS` file. The build is reproducible: built again from its tag with the tools in `packaging/release.env`, a release gives the same bytes. `packaging/release.sh --verify SHA256SUMS <version>` in the repository does that check.
