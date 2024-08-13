#!/usr/bin/env python
# Copyright 2018 ACSONE SA/NV (<http://acsone.eu>)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html).
import datetime
import json
import os
import shutil
import tempfile
import boto3
from botocore.config import Config

import click
import click_odoo
from click_odoo import odoo

from ._backup import backup
from ._dbutils import db_exists, db_management_enabled

MANIFEST_FILENAME = "manifest.json"
DBDUMP_FILENAME = "db.dump"
FILESTORE_DIRNAME = "filestore"


def _dump_db(dbname, backup):
    cmd = ["pg_dump", "--no-owner", dbname]
    filename = "dump.sql"
    if backup.format in {"dump", "folder"}:
        cmd.insert(-1, "--format=c")
        filename = DBDUMP_FILENAME
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
    db = odoo.sql_db.db_connect(dbname)
    try:
        with backup(
                format, dest, "w"
        ) as _backup, db.cursor() as cr, db_management_enabled():
            if format != "dump":
                _create_manifest(cr, dbname, _backup)
            if filestore:
                _backup_filestore(dbname, _backup)
            _dump_db(dbname, _backup)
        with db.cursor() as cr:
            _backup_s3(cr, dbname, dest)
    except Exception as e:
        with open(f"log-{dest}.txt", "w+") as f:
            f.write("======================================\n")
            f.write(str(e))
    finally:
        odoo.sql_db.close_db(dbname)


if __name__ == "__main__":  # pragma: no cover
    main()
