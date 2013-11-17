import sqlalchemy as sa

DIRECTORY_STATUSES = ['new', 'submitted', 'failed', 'running', 'grouped', 'archived']
GROUPING_STATUSES = ['new', 'submitted', 'failed', 'running', 'combined']
FILE_STATUSES = ['new', 'submitted', 'failed', 'running', 'processed', 'done', 'diagnosed', 'replaced']
FILE_OBSTYPES = ['pulsar', 'cal']
FILE_STAGES = ['combined', 'corrected', 'cleaned', 'calibrated']
CALDB_STATUSES = ['ready', 'submitted', 'updating', 'failed']

NOTELEN = 1024 # Number of characters for the note field

# Create metadata object
metadata = sa.MetaData()

# Define the metadata object
# Define versions table
sa.Table('versions', metadata, \
        sa.Column('version_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('cg_githash', sa.String(64), nullable=False), \
        sa.Column('psrchive_githash', sa.String(64), nullable=False), \
        sa.UniqueConstraint('cg_githash', 'psrchive_githash'), \
        mysql_engine='InnoDB', mysql_charset='ascii')


# Define sources table
sa.Table('sources', metadata, \
        sa.Column('source_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        mysql_engine='InnoDB', mysql_charset='ascii')


# Define directoies table
sa.Table('directories', metadata, \
        sa.Column('dir_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('path', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('status', sa.Enum(*DIRECTORY_STATUSES), nullable=False, \
                    default='new'), \
        sa.Column('note', sa.String(NOTELEN), nullable=True), \
        sa.Column('added', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('last_modified', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        mysql_engine='InnoDB', mysql_charset='ascii')


# Define groupings table
sa.Table('groupings', metadata, \
        sa.Column('group_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('dir_id', sa.Integer, \
                    sa.ForeignKey("directories.dir_id", name="fk_group_dir")), \
        sa.Column('version_id', sa.Integer, \
                    sa.ForeignKey("versions.version_id", name="fk_group_ver")), \
        sa.Column('sourcename', sa.String(32), nullable=False), \
        sa.Column('listpath', sa.String(512), nullable=False), \
        sa.Column('listname', sa.String(512), nullable=False), \
        sa.Column('status', sa.Enum(*GROUPING_STATUSES), nullable=False, \
                    default='new'), \
        sa.Column('note', sa.String(NOTELEN), nullable=True), \
        sa.Column('md5sum', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('added', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('last_modified', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.UniqueConstraint('listpath', 'listname'), \
        mysql_engine='InnoDB', mysql_charset='ascii')


# Define diagnostics table
sa.Table('diagnostics', metadata, \
        sa.Column('diagnostic_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('file_id', sa.Integer, \
                    sa.ForeignKey("files.file_id", name="fk_diag_file")), \
        sa.Column('diagnosticpath', sa.String(512), nullable=False), \
        sa.Column('diagnosticname', sa.String(512), nullable=False), \
        sa.Column('added', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('last_modified', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.UniqueConstraint('diagnosticpath', 'diagnosticname'), \
        mysql_engine='InnoDB', mysql_charset='ascii')


# Define logs table
sa.Table('logs', metadata, \
        sa.Column('log_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('group_id', sa.Integer, \
                    sa.ForeignKey("groupings.group_id", name="fk_log_group")), \
        sa.Column('logpath', sa.String(512), nullable=False), \
        sa.Column('logname', sa.String(512), nullable=False), \
        sa.Column('added', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('last_modified', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.UniqueConstraint('logpath', 'logname'), \
        sa.UniqueConstraint('group_id'), \
        mysql_engine='InnoDB', mysql_charset='ascii')


# Define groupings table
sa.Table('files', metadata, \
        sa.Column('file_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('group_id', sa.Integer, \
                    sa.ForeignKey("groupings.group_id", name="fk_files_group")), \
        sa.Column('parent_file_id', sa.Integer, \
                    sa.ForeignKey("files.file_id", name="fk_files_file")), \
        sa.Column('version_id', sa.Integer, \
                    sa.ForeignKey("versions.version_id", name="fk_files_ver")), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('filename', sa.String(512), nullable=False), \
        sa.Column('sourcename', sa.String(32), nullable=False), \
        sa.Column('status', sa.Enum(*FILE_STATUSES), nullable=False, \
                    default='new'), \
        sa.Column('note', sa.String(NOTELEN), nullable=True), \
        sa.Column('obstype', sa.Enum(*FILE_OBSTYPES), nullable=False), \
        sa.Column('stage', sa.Enum(*FILE_STAGES), nullable=False), \
        sa.Column('md5sum', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('filesize', sa.Integer, nullable=False), \
        sa.Column('is_deleted', sa.Boolean, nullable=False, \
                    default=False), \
        sa.Column('qcpassed', sa.Boolean, nullable=True, \
                    default=None), \
        sa.Column('added', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('last_modified', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.UniqueConstraint('filepath', 'filename'), \
        mysql_engine='InnoDB', mysql_charset='ascii')


# Define calibrator database table
sa.Table('caldbs', metadata, \
        sa.Column('caldb_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('caldbpath', sa.String(512), nullable=False), \
        sa.Column('caldbname', sa.String(512), nullable=False), \
        sa.Column('sourcename', sa.String(32), nullable=False, \
                    unique=True), \
        sa.Column('status', sa.Enum(*CALDB_STATUSES), nullable=False, \
                    default='ready'), \
        sa.Column('numentries', sa.Integer, nullable=False, \
                    default=0), \
        sa.Column('note', sa.String(NOTELEN), nullable=True), \
        sa.Column('added', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('last_modified', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.UniqueConstraint('caldbpath', 'caldbname'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

