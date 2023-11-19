const path = require('path');
const stream = require('stream');
const FileSystem = require('fs');
const Backup = require('./Backup');
const BackupScheduler = require('./BackupScheduler');

/**
 * @typedef {Object} DiskBackupSchedulerOptions
 * @property {string} path The path to the directory where backups will be stored.
 * @property {function():string} name This method is used to generate the name for each backup. You should always return a unique name for each backup to prevent overwriting backups.
 * @property {function(string):void|Promise<void>|Buffer|stream.Readable} prepare This method is used to prepare the content for a new backup.
 * - EITHER return a `Buffer` or `Readable` stream containing the content which will be backed up by this scheduler.
 * - OR create the backup file at the provided `path` and return a `Promise` which resolves once the backup file has been created.
 */

// Define a backup scheduler class to handle the backup process with disk storage
class DiskBackupScheduler extends BackupScheduler {
    path;
    name;
    prepare;

    /**
     * Creates a File-System / Disk storage based BackupScheduler instance.
     * - **Note**: This scheduler treats ALL files in the specified directory as backups. It is recommended to ONLY use a dedicated directory for backups.
     * @param {DiskBackupSchedulerOptions & BackupScheduler.BackupSchedulerOptions} options
     */
    constructor(options) {
        // Ensure options is a valid Object
        if (!options || typeof options !== 'object')
            throw new Error('new DiskBackupScheduler(options) -> Please provide a valid "options" Object.');

        // Ensure a path is always provided
        if (!options.path || typeof options.path !== 'string')
            throw new Error(
                'new DiskBackupScheduler(options) -> Please provide a valid "path" to the directory where backups will be stored.'
            );

        // Ensure a name generator is always provided
        if (!options.name || typeof options.name !== 'function')
            throw new Error(
                'new DiskBackupScheduler(options) -> Please provide a naming scheme for the backups. You may provide a string or a function that will be used to dynamically generate the name for each backup.'
            );

        // Ensure a prepare function is always provided
        if (!options.prepare || typeof options.prepare !== 'function')
            throw new Error(
                'new DiskBackupScheduler(options) -> Please provide a valid "prepare" function that will be used to prepare each backup.'
            );

        // Verify the path exists asynchronusly and create if it does not exist
        FileSystem.access(options.path, FileSystem.constants.F_OK, (error) => {
            // If we have an error, create the directory
            if (error) {
                FileSystem.mkdir(options.path, { recursive: true }, (error) => {
                    // If we have an error creating, emit an error event and destroy the instance
                    if (error) {
                        this.emit('error', error);
                        this.destroy();
                    }
                });
            }
        });

        // Initialize the parent class
        super({
            ...options,

            // Override the list, create, and delete methods to use the disk storage
            list: () => this.list(),
            create: () => this.create(),
            delete: (backups) => this.delete(backups),
        });

        // Store the local properties
        this.name = options.name;
        this.prepare = options.prepare;
        this.path = path.resolve(options.path);
    }

    /**
     * Lists all backups in the directory at the specified path.
     * - This method is used by the `BackupScheduler` to list all backups in the filesystem directory.
     * - **Note**: You may override this method to provide your own implementation.
     * @param {number} limit
     * @returns {Promise<false|Backup[]>}
     */
    async list(limit) {
        // Retrieve all files in the directory along with their stats
        const files = await FileSystem.promises.readdir(this.path, { recursive: false, withFileTypes: true });

        // If the number of files is less than the limit, return false to exit early for performance
        if (files.length < limit) return false;

        // Parse the files into Backup instances
        const backups = [];
        for (const file of files) {
            // Skip all non files
            if (!file.isFile()) continue;

            // Retrieve the stats for this file
            const stats = await FileSystem.promises.stat(`${this.path}/${file.name}`);

            // Create a new backup instance
            // Determine the time at which this backup was created based on the stats, account for platform inconsistencies
            backups.push(new Backup(file.name, stats.birthtimeMs || stats.mtimeMs || stats.ctimeMs));
        }

        // Return the backups
        return backups;
    }

    /**
     * Creates a new backup and uploads it to the upstream storage.
     * - This method is used by the `BackupScheduler` to create a new backup and upload it to the filesystem directory.
     * - **Note**: You may override this method to provide your own implementation.
     * @returns {Promise<void>}
     */
    async create() {
        // Generate the file name for this backup
        const name = this.name();

        // Prepare the backup
        const path = `${this.path}/${name}`;
        const preparable = await this.prepare(path);
        if (preparable instanceof stream.Readable) {
            // If the preparable is a stream, pipe it to a file and wait for it to finish
            await new Promise((resolve, reject) => {
                const file = FileSystem.createWriteStream(path);
                file.once('error', reject);
                file.once('finish', resolve);
                preparable.pipe(file);
            });
        } else if (preparable instanceof Buffer) {
            // If the preparable is a buffer, write it to the file and wait for it to finish
            await FileSystem.promises.writeFile(path, preparable);
        }
    }

    /**
     * Deletes the specified backups from the upstream storage.
     * - This method is used by the `BackupScheduler` to delete the specified backups from the filesystem directory.
     * - **Note**: You may override this method to provide your own implementation.
     * @param {Backup[]} backups
     * @returns {Promise<void>}
     */
    async delete(backups) {
        // Delete the backups concurrently
        const promises = [];
        for (const backup of backups) promises.push(FileSystem.promises.unlink(`${this.path}/${backup.id}`));
        await Promise.all(promises);
    }
}

module.exports = DiskBackupScheduler;
