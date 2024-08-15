#!/usr/bin/env python
# Copyright 2018 ACSONE SA/NV (<http://acsone.eu>)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html).
import datetime
import json
import os
import shutil
import subprocess
import tempfile
import boto3
from botocore.config import Config
import traceback
import click
import click_odoo
from click_odoo import odoo

from odoo.tools import exec_pg_environ
from ._backup import backup
from ._dbutils import db_exists, db_management_enabled

MANIFEST_FILENAME = "manifest.json"
DBDUMP_FILENAME = "db.dump"
FILESTORE_DIRNAME = "filestore"

import logging

_logger = logging.getLogger(__name__)


def dump_db_manifest(cr):
    pg_version = "%d.%d" % divmod(cr._obj.connection.server_version / 100, 100)
    cr.execute("SELECT name, latest_version FROM ir_module_module WHERE state = 'installed'")
    modules = dict(cr.fetchall())
    manifest = {
        'odoo_dump': '1',
        'db_name': cr.dbname,
        'version': odoo.release.version,
        'version_info': odoo.release.version_info,
        'major_version': odoo.release.major_version,
        'pg_version': pg_version,
        'modules': modules,
    }
    return manifest


def _odoo_basic_backup(cr, dbname, include_filestore=False, zip_filename=None):
    env = os.environ.copy()

    cmd = ["pg_dump", "--no-owner", "-U", env.get("PGUSER"), "-h", env.get("PGHOST"), "-p", "5433", dbname]
    filename = "dump.sql"
    with tempfile.TemporaryDirectory() as zip_dir:
        with tempfile.TemporaryDirectory() as dump_dir:
            cmd.insert(-1, '--file=' + os.path.join(dump_dir, filename))
            _logger.info(str(cmd))
            _logger.info(str(env))
            with open(os.path.join(dump_dir, 'manifest.json'), 'w') as fh:
                manifest = dump_db_manifest(cr)
                json.dump(manifest, fh, indent=4)

            args2 = tuple(cmd)
            process = subprocess.Popen(args2, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()  # Capture the output and error
            rc = process.returncode  # Get the return code
            if rc:
                error_message = f"Postgres subprocess {args2} error {rc}\n"
                error_message += f"Stdout:\n{stdout.decode()}\n"
                error_message += f"Stderr:\n{stderr.decode()}\n"
                raise Exception(error_message)

            if include_filestore:
                filestore = odoo.tools.config.filestore(dbname)
                if os.path.exists(filestore):
                    shutil.copytree(filestore, os.path.join(dump_dir, 'filestore'))

            command = ['7z', 'a', '-bt', '-mx=3', '-mmt=on', '-tzip', os.path.join(zip_dir, zip_filename),
                       os.path.join(dump_dir, '*')]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            while True:
                output = process.stdout.readline()
                if not output and process.poll() is not None:
                    break
                if output:
                    print(output.strip())

            # Check the return code to determine if the process was successful
            if process.returncode == 0:
                print("Compression completed successfully")

        _backup_s3(cr, dbname, os.path.join(zip_dir, zip_filename))


def _dump_db(dbname, backup):
    cmd = ["pg_dump", "--no-owner", dbname]
    filename = "dump.sql"
    if backup.format in {"dump", "folder"}:
        cmd.insert(-1, "--format=c")
        filename = DBDUMP_FILENAME
    _logger.info("PG DUMP CALL:" + str(cmd))
    env = exec_pg_environ()
    _logger.info(str(env))
    _stdin, stdout = odoo.tools.exec_pg_command_pipe(*cmd)
    backup.write(stdout, filename)


def _create_manifest(cr, dbname, backup):
    manifest = odoo.service.db.dump_db_manifest(cr)
    with tempfile.NamedTemporaryFile(mode="w") as f:
        json.dump(manifest, f, indent=4)
        f.seek(0)
        backup.addfile(f.name, MANIFEST_FILENAME)


def _backup_filestore(dbname, backup):
    filestore_source = odoo.tools.config.filestore(dbname)
    if os.path.isdir(filestore_source):
        backup.addtree(filestore_source, FILESTORE_DIRNAME)


def _backup_s3(cr, dbname, dest):
    s3_key = odoo.tools.config.get('s3_key')
    s3_secret = odoo.tools.config.get('s3_secret')
    s3_bucket = odoo.tools.config.get('s3_bucket')
    s3_region = odoo.tools.config.get('s3_region', 'us-east-1')

    _logger.info(str([s3_key, s3_secret, s3_bucket, s3_region]))

    my_config = Config(
        region_name=s3_region,
        signature_version='v4',
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        },
    )
    client = boto3.client('s3', config=my_config, aws_access_key_id=s3_key,
                          aws_secret_access_key=s3_secret)
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = "%s_%s.%s" % (dbname, ts, 'zip')
    pre = "bkdaily/"
    _logger.info(str([dest, s3_bucket, pre + filename]))
    client.upload_file(dest, Bucket=s3_bucket, Key=pre + filename)
    response = client.head_object(Bucket=s3_bucket, Key=pre + filename)
    # Get the size in bytes
    file_size_bytes = response['ContentLength']
    # Convert size to gigabytes
    file_size_gb = file_size_bytes / (1024 ** 3)

    # Asusme we have s3_backup_created table with name and size
    cr.execute(
        "INSERT INTO s3_backup_created (name,size, create_date, write_date)"
        "VALUES (%s, %s, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')",
        (filename, file_size_gb),
    )
    cr.commit()
    # clean
    if os.path.exists(dest):
        if os.path.isfile(dest):
            os.unlink(dest)
        else:
            shutil.rmtree(dest)


@click.command()
@click_odoo.env_options(
    default_log_level="warn", with_database=False, with_rollback=False
)
@click.option(
    "--force",
    is_flag=True,
    show_default=True,
    help="Don't report error if destination file/folder already exists.",
)
@click.option(
    "--if-exists", is_flag=True, help="Don't report error if database does not exist."
)
@click.option(
    "--format",
    type=click.Choice(["s3zip", "zip", "dump", "folder"]),
    default="s3zip",
    show_default=True,
    help="Output format",
)
@click.option(
    "--filestore/--no-filestore",
    default=True,
    show_default=True,
    help="Include filestore in backup",
)
@click.argument("dbname", nargs=1)
@click.argument("dest", nargs=1, required=1)
def main(env, dbname, dest, force, if_exists, format, filestore):
    """Create an Odoo database backup from an existing one.

    This script dumps the database using pg_dump.
    It also copies the filestore.

    Unlike Odoo, this script allows you to make a backup of a
    database without going through the web interface. This
    avoids timeout and file size limitation problems when
    databases are too large.

    It also allows you to make a backup directly to a directory.
    This type of backup has the advantage that it reduces
    memory consumption since the files in the filestore are
    directly copied to the target directory as well as the
    database dump.

    """
    if not db_exists(dbname):
        msg = "Database does not exist: {}".format(dbname)
        if if_exists:
            click.echo(click.style(msg, fg="yellow"))
            return
        else:
            raise click.ClickException(msg)
    if os.path.exists(dest):
        msg = "Destination already exist: {}".format(dest)
        if not force:
            raise click.ClickException(msg)
        else:
            msg = "\n".join([msg, "Remove {}".format(dest)])
            click.echo(click.style(msg, fg="yellow"))
            if os.path.isfile(dest):
                os.unlink(dest)
            else:
                shutil.rmtree(dest)
    if format == "dump":
        filestore = False
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = "%s_%s.%s" % (dbname, ts, 'zip')
    db = odoo.sql_db.db_connect(dbname)
    try:
        with db.cursor() as cr:
            _odoo_basic_backup(cr, dbname, filestore, filename)
    except Exception:
        with open(f"{dest.replace('.zip', '_log')}.txt", "w+") as f:
            f.write("======================================\n")
            f.write(str(traceback.format_exc()))
            _logger.error(traceback.format_exc())
    finally:
        odoo.sql_db.close_db(dbname)


if __name__ == "__main__":  # pragma: no cover
    main()
