# Copyright 2022 Jesper Schmitz Mouridsen.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from datetime import datetime
import json
import subprocess
import tempfile
from typing import Optional, Tuple  # noqa: H301


import eventlet
from oslo_config import cfg
from oslo_log import log as logging
import rados
import rbd

from cinder.backup import driver
from cinder import exception
import cinder.volume.drivers.rbd as rbd_driver


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
service_opts = [
    cfg.StrOpt('benji_cli_path', default='/opt/benji/bin/benji',
               help='Path to benji cli binary (in a venv)'),
    cfg.StrOpt('benji_ceph_user', default='cinder-ceph',
               help='Ceph user with access to backup_cinder_volume_pool'),
    cfg.StrOpt('benji_io_scheme', default='cinder-volumes',
               help='Benji io scheme for sources to backup'),
    cfg.StrOpt('benji_cinder_volume_pool', default='cinder-volumes',
               help='Ceph pool for benji sources to backup'),
    cfg.StrOpt('benji_cinder_volume_prefix', default='volume-',
               help='Rbd prefix for cinder volume id'),
    cfg.StrOpt('benji_cinder_snapshot_prefix', default='snapshot-',
               help='Rbd prefix for cinder snaphot   id'),
    cfg.StrOpt('benji_snapshot_prefix', default='benjibackup-',
               help='Rbd prefix for cinder snaphot id'),
    cfg.StrOpt('benji_premade_snapshot_prefix', default='benjibackup_premade-',
               help='Rbd prefix for premade cinder snaphot id'),
    cfg.StrOpt('benji_ceph_conf', default='/etc/ceph/ceph.conf',
               help='Path to ceph conf')
]
CONF.register_opts(service_opts)


class BenjiBackupDriver(driver.BackupDriver):
    """Provides backup, restore and delete using benji-backup  system."""

    def __init__(self, context):
        LOG.debug(context)
        super().__init__(context)
        self.rados = rados
        self.rbd = rbd
        self.diff_list = []

    def iterate_cb(self, offset, length, exists):
        self.diff_list.append({"offset": offset,
                               "length": length,
                               "exists": str(exists).lower()})

    def _connect_to_rados(self,
                          pool: Optional[str] = None) -> Tuple['rados.Rados',
                                                               'rados.Ioctx']:
        """Establish connection to the Ceph cluster."""
        client = eventlet.tpool.Proxy(rados.Rados(
                                      rados_id=CONF.benji_ceph_user,
                                      conffile=CONF.benji_ceph_conf
                                      ))
        try:
            client.connect()
            pool_to_open = pool or CONF.benji_cinder_volume_pool
            ioctx = client.open_ioctx(pool_to_open)
            return client, ioctx
        except rados.Error:
            # shutdown cannot raise an exception
            client.shutdown()
            raise

    @staticmethod
    def _disconnect_from_rados(client: 'rados.Rados',
                               ioctx: 'rados.Ioctx') -> None:
        """Terminate connection with the Ceph cluster."""
        # closing an ioctx cannot raise an exception
        LOG.debug("disconnect")
        ioctx.close()
        client.shutdown()

    def _rename_snap(self, source_rbd, premade_snap, snapshot):
        LOG.info("renaming '%(premade_snap)s' to '%(snap)s'", {
            'premade_snap': premade_snap,
            'snap': snapshot})
        source_rbd.rename_snap(premade_snap, snapshot)

    def _get_snap_list(self, source_rbd: rbd.Image, snap_prefix):

        snapshotlist = source_rbd.list_snaps()
        benji_snaps = [
            snapshot["name"] for snapshot in snapshotlist
            if snapshot["name"].startswith(snap_prefix)
        ]
        return benji_snaps

    def _get_snap_path(self, volume_id, snapshot, include_io_scheme=False):
        if include_io_scheme:
            return "{0}:{1}/{2}{3}@{4}".format(CONF.benji_io_scheme,
                                               CONF.benji_cinder_volume_pool,
                                               CONF.benji_cinder_volume_prefix,
                                               volume_id, snapshot)
        else:
            return '{0}/{1}{2}@{3}'.format(CONF.benji_cinder_volume_pool,
                                           CONF.benji_cinder_volume_prefix,
                                           volume_id, snapshot)

    def get_metadata(self, volume_id):
        return self.backup_meta_api.get(volume_id)

    def put_metadata(self, volume_id, json_metadata):
        self.backup_meta_api.put(volume_id, json_metadata)

    def backup(self, backup, volume_file, backup_metadata=False):
        """Start a backup of a specified volume.

        Some I/O operations may block greenthreads, so in order to prevent
        starvation parameter volume_file will be a proxy that will execute all
        methods in native threads, so the method implementation doesn't need to
        worry about that..
        """
        LOG.debug("'%(backup)s'", {'backup': backup})
        snapshot = ""
        if backup["snapshot_id"] is not None:
            snapshot = "{0}{1}".format(CONF.benji_cinder_snapshot_prefix,
                                       backup["snapshot_id"])
            subprocess.run([CONF.benji_cli_path, "backup", "-u",
                            backup.id,
                            self._get_snap_path(backup.volume_id, snapshot,
                                                include_io_scheme=True),
                            '--label',
                            f'openstack-project-id={backup.project_id}',
                            '--label', f'openstack-user-id={backup.user_id}',
                            backup.volume_id])
        elif backup['parent_id'] is None:
            # create snapshot not the snapshot specified to backup,
            # but to use for backing up the volume.
            # if a snapshot with prefix CONF.benji_premade_snapshot_prefix
            # rename and use that instead. Useful if you have taken a snapshot
            # while using fsfreeze for instance with libvirt.
            with eventlet.tpool.Proxy(rbd_driver.RADOSClient(
                    self,
                    CONF.benji_cinder_volume_pool)) as client:
                source_rbd = eventlet.tpool.Proxy(self.rbd.Image(
                    client.ioctx,
                    f'{CONF.benji_cinder_volume_prefix}{backup.volume_id}',
                    read_only=False))
                now = datetime.utcnow()
                snapshot = now.strftime(CONF.benji_snapshot_prefix +
                                        '%Y-%m-%dT%H:%M:%SZ')
                premade_snaps = self._get_snap_list(
                    source_rbd,
                    CONF.benji_premade_snapshot_prefix)
                LOG.debug(premade_snaps)
                if len(premade_snaps) > 0:
                    self._rename_snap(source_rbd, premade_snaps[-1],
                                      snapshot)
                else:
                    source_rbd.create_snap(snapshot)

                with eventlet.tpool.Proxy(self.rbd.Image(
                        client.ioctx,
                        f'{CONF.benji_cinder_volume_prefix}{backup.volume_id}',
                        snapshot=snapshot,
                        read_only=False)) as source_rbd_snap:
                    self.diff_list = []
                    source_rbd_snap.diff_iterate(
                        0,
                        source_rbd.size(),
                        None, self.iterate_cb, whole_object=True)
                    json_hints = tempfile.NamedTemporaryFile(mode="w+")
                    json.dump(self.diff_list,
                              json_hints, separators=(',', ':'))
                    json_hints.flush()
                    subprocess.run([
                        CONF.benji_cli_path, "backup", "-r",
                        json_hints.name, "-u", backup.id,
                        self._get_snap_path(
                            backup.volume_id, snapshot,
                            include_io_scheme=True),
                        '--label', f'openstack-project-id={backup.project_id}',
                        '--label', f'openstack-user-id={backup.user_id}',
                        backup.volume_id,
                        '--snapshot', snapshot])
                    LOG.debug("done")
                    source_rbd.close()
        elif backup['parent_id'] is not None:
            with eventlet.tpool.Proxy(rbd_driver.RADOSClient(
                    self,
                    CONF.benji_cinder_volume_pool)) as client:
                with eventlet.tpool.Proxy(self.rbd.Image(
                        client.ioctx,
                        f'{CONF.benji_cinder_volume_prefix}{backup.volume_id}',
                        read_only=False)) as source_rbd:
                    benji_snaps_unsorted = self._get_snap_list(
                        source_rbd,
                        CONF.benji_snapshot_prefix)
                    # sort the list by  name without the prefix, i.e by date.
                    benji_snaps = sorted(benji_snaps_unsorted,
                                         key=lambda x: x.split(
                                             CONF.benji_snapshot_prefix)[1])
                    LOG.debug(benji_snaps)
                    for delete_snapname in benji_snaps[:-1]:
                        source_rbd.remove_snap(delete_snapname)
                    last_snapshot = benji_snaps[-1]
                    result = subprocess.run([
                        CONF.benji_cli_path, '--machine-output', 'ls',
                        f'volume == "{backup.volume_id}"\
                            and snapshot == "{last_snapshot}"\
                            and status == "valid"'],
                        capture_output=True)
                    versions = json.loads(result.stdout)
                    LOG.debug(result.stderr)
                    LOG.debug(result.stdout)
                    LOG.debug(versions)
                    if len(versions["versions"]) == 0:
                        raise exception.BackupDriverException(
                            reason = 'latest snapshot is not in ' +
                                     'benji db please fallback to full backup')
                    LOG.debug(versions["versions"][0]["uid"])
                    now = datetime.utcnow()
                    snapshot = now.strftime(CONF.benji_snapshot_prefix +
                                            '%Y-%m-%dT%H:%M:%SZ')
                    premade_snaps = self._get_snap_list(
                        source_rbd,
                        CONF.benji_premade_snapshot_prefix)
                    LOG.debug(premade_snaps)
                    if len(premade_snaps) > 0:
                        self._rename_snap(source_rbd,
                                          premade_snaps[-1],
                                          snapshot)
                    else:
                        source_rbd.create_snap(snapshot)

                    with eventlet.tpool.Proxy(
                        self.rbd.Image(
                            client.ioctx,
                            f'{CONF.benji_cinder_volume_prefix}' +
                            f'{backup.volume_id}',
                            snapshot=snapshot,
                            read_only=False)) as source_rbd_snap:
                        self.diff_list = []
                        source_rbd_snap.diff_iterate(
                            0,
                            source_rbd_snap.size(),
                            last_snapshot, self.iterate_cb, whole_object=True)
                        hints_file = tempfile.NamedTemporaryFile(mode="w+")
                        json.dump(self.diff_list,
                                  hints_file, separators=(',', ':'))
                        hints_file.flush()
                        source_rbd.remove_snap(last_snapshot)
                        LOG.debug(str(subprocess.run([
                            CONF.benji_cli_path, 'backup',
                            '--snapshot', snapshot,
                            '--rbd-hints', hints_file.name,
                            '--base-version',
                            versions["versions"][0]["uid"],
                            '--uid', backup.id,
                            '--label',
                            f'openstack-project-id={backup.project_id}',
                            '--label', f'openstack-user-id={backup.user_id}',
                            self._get_snap_path(backup.volume_id,
                                                snapshot,
                                                include_io_scheme=True),
                            backup.volume_id],
                            capture_output=False)))
                        source_rbd_snap.close()
                        source_rbd.close()
                        return {}

    def restore(self, backup, volume_id, volume_file):
        """Restore a saved backup.

        Some I/O operations may block greenthreads, so in order to prevent
        G
        starvation parameter volume_file will be a proxy that will execute all
        methods in native threads, so the method implementation doesn't need to
        worry about that..

        May raise BackupRestoreCancel to indicate that the restoration of a
        volume has been aborted by changing the backup status.
        """
        LOG.debug(subprocess.run(
            [
                CONF.benji_cli_path,
                "restore", "--sparse", "--force",
                backup.id,
                "{0}:{1}/{2}{3}".format(
                    CONF.benji_io_scheme,
                    CONF.benji_cinder_volume_pool,
                    CONF.benji_cinder_volume_prefix,
                    volume_id)]))
        return

    def delete_backup(self, backup):
        """Delete a saved backup."""
        LOG.debug(subprocess.run(
            [
                CONF.benji_cli_path,
                "rm",
                backup.id,
                "--force"]))
        return
        return

    def export_record(self, backup):
        """Export driver specific backup record information.

        If backup backend needs additional driver specific information to
        import backup record back into the system it must overwrite this method
        and return it here as a dictionary so it can be serialized into a
        string.

        Default backup driver implementation has no extra information.

        :param backup: backup object to export
        :returns: driver_info - dictionary with extra information
        """
        return {}

    def import_record(self, backup, driver_info):
        """Import driver specific backup record information.

        If backup backend needs additional driver specific information to
        import backup record back into the system it must overwrite this method
        since it will be called with the extra information that was provided by
        export_record when exporting the backup.

        Default backup driver implementation does nothing since it didn't
        export any specific data in export_record.

        :param backup: backup object to export
        :param driver_info: dictionary with driver specific backup record
                            information
        :returns: nothing
        """
        return

    def check_for_setup_error(self):
        return
