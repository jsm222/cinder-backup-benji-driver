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
from typing import Optional, Tuple  # noqa: H301

from benji.benji import Benji
from benji.benji import VersionUid
import benji.config as benji_config
from benji.io.factory import IOFactory
from benji.storage.factory import StorageFactory
import eventlet
from oslo_config import cfg
from oslo_log import log as logging
import rados
import rbd

from cinder.backup import driver
from cinder import exception
import cinder.volume.drivers.rbd as rbd_driver


def setup_logging():

    extra_log_level_defaults = [
        'dogpile=INFO',
        'routes=INFO',
        'benji=INFO'
    ]

    logging.set_defaults(
        default_log_levels=logging.get_default_log_levels() +
        extra_log_level_defaults)

    logging.setup(CONF, "cinder-backup")


service_opts = [
    cfg.StrOpt('benji_cli_path', default='/opt/benji/bin/benji',
               help='Path to benji cli binary (in a venv)'),
    cfg.StrOpt('benji_io_scheme_ceph', default='cinder-volumes',
               help='Benji io scheme for ceph sources to backup'),
    cfg.StrOpt('benji_io_scheme_lvm', default='file',
               help='Benji io scheme for ceph sources to backup'),
    cfg.StrOpt('benji_snapshot_prefix', default='benjibackup-',
               help='Rbd prefix for cinder snaphot id'),
    cfg.StrOpt('benji_premade_snapshot_prefix', default='benjibackup_premade-',
               help='Rbd prefix for premade cinder snaphot id'),
]
CONF = cfg.CONF
CONF.register_opts(service_opts)
setup_logging()
LOG = logging.getLogger(__name__)


class BenjiBackupDriver(driver.BackupDriver):

    """Provides backup, restore and delete using benji-backup system."""

    def __init__(self, context, db=None):
        super().__init__(context)
        self.rados = rados
        self.rbd = rbd
        self.diff_list = []
        bconfig = benji_config.Config()
        IOFactory.initialize(bconfig)
        StorageFactory.initialize(bconfig)
        self.b_backup = Benji(bconfig)

    def iterate_cb(self, offset, length, exists):
        self.diff_list.append((offset, length, exists))

    def _connect_to_rados(self,
                          pool: Optional[str] = None) -> Tuple['rados.Rados',
                                                               'rados.Ioctx']:
        """Establish connection to the Ceph cluster."""
        client = eventlet.tpool.Proxy(rados.Rados(
                                      rados_id=self.ceph_user,
                                      conffile=self.ceph_conf
                                      ))
        try:
            client.connect()
            pool_to_open = pool or "cinder-volumes"
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
        image_name = CONF.volume_name_template % volume_id
        if include_io_scheme:
            return "{0}:{1}/{2}@{3}".format(self.io_scheme,
                                            self.pool,
                                            image_name,
                                            snapshot)
        else:
            return '{0}/{1}@{2}'.format(self.pool, image_name, snapshot)

    def get_metadata(self, volume_id):
        return self.backup_meta_api.get(volume_id)

    def put_metadata(self, volume_id, json_metadata):
        self.backup_meta_api.put(volume_id, json_metadata)

    def _parse_backend(self, volume_id):
        # Using DEFAULT section to configure drivers
        # is not supported since Ocata.
        if len(CONF.enabled_backends) > 1:
            src_volume = self.db.volume_get(self.context, volume_id)
            backend = [b.value
                       for b in src_volume.volume_type.extra_specs
                       if b.key == "volume_backend_name"]
            LOG.debug(src_volume.volume_type.extra_specs)
            LOG.debug("'%(backend)s'", {'backend': backend})

        else:
            backend = CONF.enabled_backends
        return backend

    def _backup_rbd_differential(self, backup):
        with eventlet.tpool.Proxy(rbd_driver.RADOSClient(
                self, self.pool)) as client:
            with eventlet.tpool.Proxy(self.rbd.Image(
                    client.ioctx,
                    CONF.volume_name_template % backup.volume_id,
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
                versions = self.b_backup.find_versions_with_filter(
                    f'volume == "{backup.volume_id}"\
                           and snapshot == "{last_snapshot}"\
                           and status == "valid"')
                if len(versions) == 0:
                    raise exception.BackupDriverException(
                        reason = 'latest snapshot is not in ' +
                                 'benji db please fallback to full backup')
                LOG.debug(versions[0].uid)
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

                with eventlet.tpool.Proxy(
                    self.rbd.Image(
                        client.ioctx,
                        CONF.volume_name_template % backup.volume_id,
                        snapshot=snapshot,
                        read_only=False)) as source_rbd_snap:
                    self.diff_list = []
                    source_rbd_snap.diff_iterate(
                        0, source_rbd_snap.size(),
                        last_snapshot, self.iterate_cb, whole_object=True)
                    source_rbd.remove_snap(last_snapshot)
                    self.b_backup.backup(version_uid=backup.id,
                                         base_version_uid=versions[0].uid,
                                         volume=backup.volume_id,
                                         snapshot=snapshot,
                                         source=self._get_snap_path(
                                             backup.volume_id,
                                             snapshot,
                                             include_io_scheme=True),

                                         hints=self.diff_list)
                    self._add_labels(backup)
                    source_rbd_snap.close()
                    source_rbd.close()
                    return

    def _backup_rbd_initial(self, backup):
        with eventlet.tpool.Proxy(rbd_driver.RADOSClient(
                self, self.pool)) as client:
            with eventlet.tpool.Proxy(self.rbd.Image(
                    client.ioctx,
                    CONF.volume_name_template % backup.volume_id,
                    read_only=False)) as source_rbd:
                now = datetime.utcnow()
                snapshot = now.strftime(CONF.benji_snapshot_prefix +
                                        '%Y-%m-%dT%H:%M:%SZ')
                premade_snaps = self._get_snap_list(
                    source_rbd,
                    CONF.benji_premade_snapshot_prefix)

                LOG.debug(premade_snaps)
                if len(premade_snaps) > 0:
                    self._rename_snap(source_rbd, premade_snaps[-1], snapshot)
                else:
                    LOG.debug(source_rbd.get_name())
                    LOG.debug(source_rbd.create_snap(snapshot))
                with eventlet.tpool.Proxy(self.rbd.Image(
                        client.ioctx,
                        CONF.volume_name_template % backup.volume_id,
                        snapshot=snapshot,
                        read_only=False)) as source_rbd_snap:
                    self.diff_list = []
                    source_rbd_snap.diff_iterate(0, source_rbd.size(),
                                                 None,
                                                 self.iterate_cb,
                                                 whole_object=True)
                    self.b_backup.backup(version_uid=backup.id,
                                         volume=backup.volume_id,
                                         snapshot=snapshot,
                                         source=self._get_snap_path(
                                             backup.volume_id,
                                             snapshot,
                                             include_io_scheme=True),

                                         hints=self.diff_list)
                    self._add_labels(backup)
                    LOG.debug("done")
                    source_rbd_snap.close()
                    source_rbd.close()
                    return

    def _add_labels(self, backup):
        Benji.add_label(version_uid=backup.id,
                        key='openstack-project-id', value=backup.project_id)
        Benji.add_label(version_uid=backup.id, key='openstack-user-id',
                        value=backup.user_id)

    def _backup_lvm(self, backup, volume_file):
        source = f'{self.io_scheme}:{volume_file._obj.name}'
        self.b_backup.backup(version_uid=backup.id, source=source, snapshot="",
                             volume=backup.volume_id)
        self._add_labels(backup)

    def backup(self, backup, volume_file, backup_metadata=False):
        """Start a backup of a specified volume.

        Some I/O operations may block greenthreads, so in order to prevent
        starvation parameter volume_file will be a proxy that will execute all
        methods in native threads, so the method implementation doesn't need to
        worry about that..
        """
        backend = self._parse_backend(backup.volume_id)
        if hasattr(volume_file, 'rbd_image'):
            self.io_scheme = CONF.benji_io_scheme_ceph
            CONF.register_opts(rbd_driver.RBD_OPTS, group=backend[0])
            LOG.debug(CONF[backend[0]].rbd_pool)
            self.pool = CONF[backend[0]].rbd_pool
            self.ceph_user = CONF[backend[0]].rbd_user
            self.ceph_conf = CONF[backend[0]].rbd_ceph_conf

        else:
            self.io_scheme = CONF.benji_io_scheme_lvm

        if backup["snapshot_id"] is not None:
            if hasattr(volume_file, 'rbd_image'):
                ceph_name = volume_file.rbd_image.image.get_name()
                source = f'{self.io_scheme}:{self.pool}/{ceph_name}'
            else:
                dev_file = volume_file._obj.name
                source = f'{CONF.benji_io_scheme_lvm}:{dev_file}'

            self.b_backup.backup(version_uid=backup.id, source=source,
                                 snapshot="", volume=backup.volume_id)
            self._add_labels(backup)
        elif backup['parent_id'] is None:
            if self.io_scheme == CONF.benji_io_scheme_ceph:
                self._backup_rbd_initial(backup)
            elif self.io_scheme == CONF.benji_io_scheme_lvm:
                self._backup_lvm(backup, volume_file)
        elif backup['parent_id'] is not None:
            if self.io_scheme == CONF.benji_io_scheme_ceph:
                self._backup_rbd_differential(backup)
            elif self.io_scheme == CONF.benji_io_scheme_lvm:
                self._backup_lvm(backup, volume_file)

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
        backend = self._parse_backend(volume_id)
        force = True
        sparse = False
        if hasattr(volume_file, 'rbd_image'):
            image_name = volume_file.rbd_image.image.get_name()
            CONF.register_opts(rbd_driver.RBD_OPTS, group=backend[0])
            pool = CONF[backend[0]].rbd_pool
            target = f'{CONF.benji_io_scheme_ceph}:{pool}/{image_name}'
            sparse = True
        else:
            target = f'{CONF.benji_io_scheme_lvm}:{volume_file._obj.name}'

        LOG.debug(self.b_backup.restore(version_uid=backup.id,
                                        target=target,
                                        sparse=sparse,
                                        force=force))
        return

    def delete_backup(self, backup):
        """Delete a saved backup."""
        try:
            self.b_backup.rm(VersionUid(backup.id), force=True)
        except KeyError:
            LOG.warning("'%(version_uid)s' not found in benji database",
                        {'version_uid': backup.id})
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
