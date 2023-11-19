const stream = require('stream');
const Backup = require('./Backup');
const AWS = require('@aws-sdk/client-s3');
const BackupScheduler = require('./BackupScheduler');

/**
 * @typedef {Object} S3BackupSchedulerOptions
 * @property {AWS.S3ClientConfig['region']} region The region of the S3 Bucket / Block storage to use for backups.
 * @property {string} bucket The name of the S3 Bucket / Block storage to use for backups.
 * @property {string} accessKeyId The access key ID for the S3 Bucket / Block storage.
 * @property {string} secretAccessKey The secret access key for the S3 Bucket / Block storage.
 * @property {AWS.S3ClientConfig} [client] The options to use when creating the S3 client.
 * @property {function():string} name This method is used to generate the name for each backup. You should always return a unique name for each backup to prevent overwriting backups.
 * @property {function(string):void|Promise<void>|Buffer|stream.Readable} prepare This method is used to prepare / upload the content for a new backup.
 * - EITHER return a `Buffer` or `Readable` stream containing the content which will be backed up to the cloud storage.
 * - OR upload the backup file at the provided `name` and return a `Promise` which resolves once the backup file has been uploaded.
 */

// Define a backup scheduler class to handle the backup process with S3 Buckets / Block storage
class S3BackupScheduler extends BackupScheduler {
    name;
    bucket;
    prepare;

    /**
     * @type {AWS.S3}
     */
    client;

    /**
     * Creates a S3 Bucket / Block storage based BackupScheduler instance.
     * - **Note**: This scheduler treats ALL items in the specified S3 Bucket as backup files.
     * - It is recommended to ONLY use a dedicated Bucket for backups.
     * - OR override / intercept the `list` method to filter backups related to this scheduler only.
     * @param {S3BackupSchedulerOptions & BackupScheduler.BackupSchedulerOptions} options
     */
    constructor(options) {
        // Ensure options is a valid Object
        if (!options || typeof options !== 'object')
            throw new Error('new S3BackupScheduler(options) -> Please provide a valid "options" Object.');

        // Ensure a bucket is always provided
        if (!options.bucket || typeof options.bucket !== 'string')
            throw new Error('new S3BackupScheduler(options) -> Please provide a valid bucket name to use for backups.');

        // Ensure an access key ID is always provided
        if (!options.accessKeyId || typeof options.accessKeyId !== 'string')
            throw new Error(
                'new S3BackupScheduler(options) -> Please provide a valid access key ID to access the S3 Bucket / Block storage.'
            );

        // Ensure a secret access key is always provided
        if (!options.secretAccessKey || typeof options.secretAccessKey !== 'string')
            throw new Error(
                'new S3BackupScheduler(options) -> Please provide a valid secret access key to access the S3 Bucket / Block storage.'
            );

        // Ensure a region is always provided
        if (!options.region || typeof options.region !== 'string')
            throw new Error(
                'new S3BackupScheduler(options) -> Please provide a valid region to use for the S3 Bucket / Block storage.'
            );

        // Ensure a name generator is always provided
        if (!options.name || typeof options.name !== 'function')
            throw new Error(
                'new S3BackupScheduler(options) -> Please provide a naming scheme for the backups. You may provide a string or a function that will be used to dynamically generate the name for each backup.'
            );

        // Ensure a prepare function is always provided
        if (!options.prepare || typeof options.prepare !== 'function')
            throw new Error(
                'new S3BackupScheduler(options) -> Please provide a valid "prepare" function that will be used to prepare each backup.'
            );

        // Initialize the parent class
        super({
            ...options,

            // Redact any sensitive information about the S3 Bucket / Block storage from the logs which the options property may show up in
            region: '*******',
            bucket: '*******',
            accessKeyId: '*******',
            secretAccessKey: '*******',

            // Override the list, create, and delete methods to use the S3 Bucket / Block storage
            list: () => this.list(),
            create: () => this.create(),
            delete: (backups) => this.delete(backups),
        });

        // Store the local properties
        this.name = options.name;
        this.prepare = options.prepare;

        // Initialize the S3 client
        this.bucket = options.bucket;
        this.client = new AWS.S3({
            ...options.client,
            region: options.region,
            credentials: {
                ...options.client?.credentials,
                accessKeyId: options.accessKeyId,
                secretAccessKey: options.secretAccessKey,
            },
        });
    }

    /**
     * Lists all backups in the S3 Bucket / Block storage.
     * - This method is used by the `BackupScheduler` to list all backups in the S3 Bucket / Block storage.
     * - **Note**: You may override this method to provide your own implementation.
     * @param {number} limit
     * @param {string} [ContinuationToken]
     * @returns {Promise<false|Backup[]>}
     */
    async list(limit, ContinuationToken) {
        // List the files from the S3 Bucket / Block storage
        const response = await this.client.listObjectsV2({
            Bucket: this.bucket,
            ContinuationToken,
        });

        // Retrieve various properties from the response
        const count = response.KeyCount || 0;
        const files = response.Contents || [];
        const more = (response.IsTruncated && response.NextContinuationToken) || false;

        // If there are no more results, this wasn't a continuation, and there are no files, return false early to save on API calls
        if (!more && !ContinuationToken && count < limit) return false;

        // Parse the files into Backup instances
        const backups = [];
        for (const file of files) {
            // Create a new backup instance
            backups.push(new Backup(file.Key, file.LastModified.getTime()));
        }

        // If there are more results, recursively fetch them and append them to the backups array
        if (more) {
            const more_backups = await this.list(limit, response.NextContinuationToken);
            if (more_backups) backups.push(...more_backups);
        }

        // Return the backups
        return backups;
    }

    /**
     * Creates a new backup and uploads it to the upstream storage.
     * - This method is used by the `BackupScheduler` to create a new backup and upload it to the S3 Bucket / Block storage.
     * - **Note**: You may override this method to provide your own implementation.
     * @returns {Promise<void>}
     */
    async create() {
        // Prepare the backup
        const name = this.name();
        const preparable = await this.prepare(name);
        if (preparable instanceof Buffer || preparable instanceof stream.Readable) {
            // If the preparable is a Readable stream or Buffer, upload it to the S3 Bucket / Block storage
            const command = new AWS.PutObjectCommand({
                Key: name,
                Bucket: this.bucket,
                Body: preparable,
            });

            // Upload the backup to the S3 Bucket / Block storage
            await this.client.send(command);
        }
    }

    /**
     * Deletes the specified backups from the upstream storage.
     * - This method is used by the `BackupScheduler` to delete the specified backups from the S3 Bucket / Block storage.
     * - **Note**: You may override this method to provide your own implementation.
     * @param {Backup[]} backups
     * @returns {Promise<void>}
     */
    async delete(backups) {
        // Delete the backups from the S3 Bucket / Block storage
        await this.client.deleteObjects({
            Bucket: this.bucket,
            Delete: {
                Objects: backups.map((backup) => ({ Key: backup.id })),
            },
        });
    }
}

module.exports = S3BackupScheduler;
