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

from oslo_config import cfg
from oslo_log import log as logging

from cinder.backup import driver
from cinder import exception
from cinder.message import message_field
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
               help='Rbd prefix for cinder snaphot   id'),
    cfg.StrOpt('benji_premade_snapshot_prefix', default='benjibackup_premade-',
               help='Rbd prefix for cinder snaphot   id'),
]
CONF.register_opts(service_opts)


class BenjiBackupDriver(driver.BackupDriver):
    """Provides backup, restore and delete using benji-backup  system."""

    def __init__(self, context):
        if context.message_action is None:
            context.message_action = message_field.Action.BACKUP_CREATE
        super().__init__(context)

    def _rename_snap(self, volume_id, premade_snap, snapshot):
        premade_snappath = self._get_snap_path(volume_id,
                                               premade_snap,
                                               include_io_scheme=False)

        snap_path = self._get_snap_path(volume_id,
                                        snapshot,
                                        include_io_scheme=False)
        LOG.info("renaming '%(premade_snap)s' to '%(snap)s'", {
            'premade_snap': premade_snappath,
            'snap': snap_path,
        })
        subprocess.run(['rbd', 'snap', 'rename',
                        premade_snappath,
                        snap_path,
                        '--id', CONF.benji_ceph_user])

    def _get_snap_list(self, volume_id, snap_prefix):
        snaps = subprocess.run(['rbd', 'snap', 'ls', '--format=json',
                                '{0}/{1}{2}'.format(
                                    CONF.benji_cinder_volume_pool,
                                    CONF.benji_cinder_volume_prefix,
                                    volume_id),
                                '--id',
                                CONF.benji_ceph_user],
                               capture_output=True)
        snapshotlist = json.loads(snaps.stdout)
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
        if backup["snapshot_id"] is not None:
            snapshot = "{0}{1}".format(CONF.benji_snaphost_prefix,
                                       backup["snapshot_id"])
            subprocess.run([CONF.benji_cli_path, "backup", "-u",
                            backup.id,
                            self._get_snap_path(backup.volume_id, snapshot,
                                                include_io_scheme=False),
                            backup.volume_id])
        elif backup['parent_id'] is None:
            snapshot = ""
            # create snapshot not the snapshot specified to backup,
            # but to use for backing up the volume.
            # if a snapshot with prefix CONF.benji_premade_snapshot_prefix
            # rename and use that instead. Useful if you have taken a snapshot
            # while using fsfreeze for instance with libvirt.
            now = datetime.utcnow()
            snapshot = now.strftime(CONF.benji_snapshot_prefix +
                                    '%Y-%m-%dT%H:%M:%SZ')
            premade_snaps = self._get_snap_list(
                backup.volume_id,
                CONF.benji_premade_snapshot_prefix)
            LOG.debug(premade_snaps)
            if len(premade_snaps) > 0:
                self._rename_snap(backup.volume_id,
                                  premade_snaps[-1],
                                  snapshot)
            else:
                subprocess.run(['rbd', 'snap', 'create', '--no-progress',
                                self._get_snap_path(
                                    backup.volume_id,
                                    snapshot,
                                    include_io_scheme=False),
                                '--id', CONF.benji_ceph_user])

            result = subprocess.run(['rbd', 'diff', '--whole-object',
                                     '--format=json',
                                     self._get_snap_path(
                                         backup.volume_id,
                                         snapshot,
                                         include_io_scheme=False),
                                     '--id', CONF.benji_ceph_user],
                                    capture_output=True)
            json_hints = tempfile.NamedTemporaryFile()
            json_hints.write(result.stdout)
            json_hints.flush()
            subprocess.run([CONF.benji_cli_path, "backup", "-r",
                            json_hints.name, "-u",
                            backup.id,
                            self._get_snap_path(backup.volume_id, snapshot,
                                                include_io_scheme=True),
                            backup.volume_id,
                            '--snapshot', snapshot])
        elif backup['parent_id'] is not None:
            if not backup.volume_id == backup.parent.volume_id:
                LOG.error("backup volume_id: %(volume_id)" +
                          "!= backup.parent.volume_id %(parent_id)",
                          {'volume_id': backup.volume_id,
                           'parent_id': backup.parent.volume_id})
            else:
                benji_snaps_id_sorted = self._get_snap_list(
                    backup.volume_id,
                    CONF.benji_snapshot_prefix)
                # sort the list by  name without the prefix, i.e by date.
                benji_snaps = sorted(benji_snaps_id_sorted,
                                     key=lambda x: x.split(
                                         CONF.benji_snapshot_prefix)[1])
                LOG.debug(benji_snaps)
                for delete_snapname in benji_snaps[:-1]:
                    subprocess.run(['rbd', 'snap', 'rm',
                                    self._get_snap_path(
                                        backup.volume_id,
                                        delete_snapname,
                                        include_io_scheme=False),
                                    '--id', CONF.benji_ceph_user])
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
                    backup.volume_id,
                    CONF.benji_premade_snapshot_prefix)
                LOG.debug(premade_snaps)
                if len(premade_snaps) > 0:
                    self._rename_snap(backup.volume_id,
                                      premade_snaps[-1],
                                      snapshot)
                else:
                    LOG.debug(subprocess.run(['rbd', 'snap', 'create',
                                              '--no-progress',
                                              self._get_snap_path(
                                                  backup.volume_id,
                                                  snapshot,
                                                  include_io_scheme=False),
                                              '--id', CONF.benji_ceph_user]))
                hintscmd = subprocess.run(['rbd', 'diff', '--whole-object',
                                           '--format=json', '--from-snap',
                                           last_snapshot, self._get_snap_path(
                                               backup.volume_id,
                                               snapshot,
                                               include_io_scheme=False),
                                           '--id', CONF.benji_ceph_user],
                                          capture_output=True)
                hints_file = tempfile.NamedTemporaryFile()
                hints_file.write(hintscmd.stdout)
                hints_file.flush()
                LOG.debug(hintscmd.stdout)
                LOG.debug(hintscmd.stderr)
                subprocess.run(['rbd', 'snap', 'rm', '--no-progress',
                                self._get_snap_path(backup.volume_id,
                                                    last_snapshot,
                                                    include_io_scheme=False),
                                '--id', CONF.benji_ceph_user])
                LOG.debug(str(subprocess.run([
                    CONF.benji_cli_path, 'backup',
                    '--snapshot', snapshot,
                    '--rbd-hints', hints_file.name,
                    '--base-version',
                    versions["versions"][0]["uid"],
                    '--uid',
                    backup.id,
                    self._get_snap_path(backup.volume_id,
                                        snapshot,
                                        include_io_scheme=True),
                    backup.volume_id],
                    capture_output=False)))

        return

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
