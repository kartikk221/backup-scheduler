const Backup = require('./src/Backup');
const BackupScheduler = require('./src/BackupScheduler');
const S3BackupScheduler = require('./src/S3BackupScheduler');
const DiskBackupScheduler = require('./src/DiskBackupScheduler');

module.exports = {
    Backup,
    BackupScheduler,
    S3BackupScheduler,
    DiskBackupScheduler,
};
